import os
import sys
import codecs
import pandas as pd
import numpy as np

# Ép hệ thống dùng chuẩn UTF-8 khi in ra màn hình (Fix lỗi Unicode trên terminal Windows)
if sys.stdout.encoding != 'utf-8':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

import clickhouse_connect
import mlflow
import mlflow.statsmodels
from mlflow.models import infer_signature
from statsmodels.tsa.arima.model import ARIMA
from sklearn.metrics import mean_absolute_error, mean_squared_error
from mlflow.tracking import MlflowClient

# ==========================================
# CẤU HÌNH KẾT NỐI
# ==========================================
CH_HOST = os.getenv('CLICKHOUSE_HOST', 'qlfb8ypu5w.ap-northeast-1.aws.clickhouse.cloud')
CH_PORT = int(os.getenv('CLICKHOUSE_PORT', '8443'))
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'N7f8bLl.qrbON')
CH_DB = 'hospital_dw'

# Tracking URI tới container MLflow vừa được cấu hình trong Docker Compose
# Nếu chạy trong Airflow Docker, sẽ trỏ tới http://mlflow-server:5000. Nếu chạy Local thì localhost:5000
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = "Revenue_Forecasting"
MODEL_NAME = "ARIMA_Revenue_Model"


def get_data_from_clickhouse():
    """
    Kết nối ClickHouse, lấy doanh thu theo ngày và sắp xếp tăng dần.
    """
    print("1. Đang kết nối tới ClickHouse...")
    client = clickhouse_connect.get_client(
        host=CH_HOST, 
        port=CH_PORT, 
        username=CH_USER, 
        password=CH_PASSWORD, 
        secure=True
    )
    
    query = """
    SELECT 
        DateKey, 
        SUM(RevenueAmount) as daily_revenue
    FROM hospital_dw.fact_service_revenue
    GROUP BY DateKey
    ORDER BY DateKey ASC
    """
    print("   Đang query dữ liệu fact_service_revenue...")
    df = client.query_df(query)
    
    if df.empty:
        raise ValueError("Không có dữ liệu trong bảng fact_service_revenue!")
    
    # Tiền xử lý DateKey (dạng Int32: 20230101) sang Datetime
    df['Date'] = pd.to_datetime(df['DateKey'].astype(str), format='%Y%m%d')
    # Đảm bảo sắp xếp đúng thứ tự ngày tăng dần (theo yêu cầu)
    df = df.sort_values('Date').reset_index(drop=True)
    df.set_index('Date', inplace=True)
    
    # Chỉ lấy cột doanh thu
    df = df[['daily_revenue']]
    # Ép kiểu Decimal/Object về float64 cho statsmodels
    df['daily_revenue'] = pd.to_numeric(df['daily_revenue'], errors='coerce')
    # Xử lý missing dates nếu có (fill 0)
    df = df.asfreq('D', fill_value=0)
    
    return df


def main():
    df = get_data_from_clickhouse()
    print(f"   Lấy thành công {len(df)} ngày dữ liệu lịch sử.")
    
    # ==========================================
    # CHIA TRAIN / TEST
    # ==========================================
    test_size = 30 # Dự đoán 30 ngày
    if len(df) <= test_size:
        raise ValueError("Dữ liệu quá ít để chia tập test 30 ngày.")
        
    train_data = df.iloc[:-test_size]
    test_data = df.iloc[-test_size:]
    
    print(f"2. Đã chia dữ liệu: Train ({len(train_data)} ngày) - Test ({len(test_data)} ngày)")
    
    # ==========================================
    # KHỞI TẠO MLFLOW EXPERIMENT
    # ==========================================
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    print(f"3. Bắt đầu quá trình Train và Log vào MLflow ({MLFLOW_TRACKING_URI})...")
    with mlflow.start_run(log_system_metrics=False) as run:
        # Khởi tạo mô hình ARIMA (Tham số giả định p=5, d=1, q=0)
        p, d, q = 5, 1, 0
        model = ARIMA(train_data['daily_revenue'], order=(p, d, q))
        fitted_model = model.fit()
        
        # Dự báo trên tập test
        predictions = fitted_model.forecast(steps=test_size)
        
        # Đánh giá độ chính xác (Metrics)
        mae = mean_absolute_error(test_data['daily_revenue'], predictions)
        rmse = np.sqrt(mean_squared_error(test_data['daily_revenue'], predictions))
        
        print(f"   -> Kết quả Test: MAE = {mae:,.2f} | RMSE = {rmse:,.2f}")
        
        # Log tham số và kết quả vào MLflow
        mlflow.log_param("p", p)
        mlflow.log_param("d", d)
        mlflow.log_param("q", q)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        
        # Tạo schema (signature) cho model dựa vào dữ liệu train và 1 kết quả dự đoán
        signature = infer_signature(train_data, fitted_model.forecast(steps=1))
        
        # Log model (có kèm theo Signature/Schema)
        mlflow.statsmodels.log_model(
            statsmodels_model=fitted_model,
            artifact_path="model",
            signature=signature
        )
        
        run_id = run.info.run_id
        
        # ==========================================
        # MLOPS: SO SÁNH & REGISTER MODEL (CI/CD cho Model)
        # ==========================================
        print("4. Đang kiểm tra Model Registry để so sánh độ chính xác...")
        client = MlflowClient()
        
        try:
            # Lấy tất cả version của model này
            latest_versions = client.get_latest_versions(MODEL_NAME)
            
            if len(latest_versions) > 0:
                # Lấy version mới nhất đang có
                latest_version = sorted(latest_versions, key=lambda v: int(v.version), reverse=True)[0]
                run_id_of_registered = latest_version.run_id
                
                # Lấy MAE của model cũ từ trong lịch sử MLflow
                old_metrics = client.get_run(run_id_of_registered).data.metrics
                old_mae = old_metrics.get("mae", float('inf'))
                
                print(f"   -> Model hiện tại (Version {latest_version.version}) có MAE: {old_mae:,.2f}")
                
                if mae < old_mae:
                    print("   -> TỐT HƠN! Mô hình mới có sai số (MAE) nhỏ hơn. Đang tiến hành Register...")
                    # Register model mới
                    model_uri = f"runs:/{run_id}/model"
                    mv = mlflow.register_model(model_uri, MODEL_NAME)
                    
                    # Thêm Description (mô tả) cho Model
                    client.update_model_version(
                        name=MODEL_NAME,
                        version=mv.version,
                        description="Mô hình ARIMA dự báo doanh thu dịch vụ 30 ngày tiếp theo. Dữ liệu huấn luyện từ ClickHouse `fact_service_revenue`."
                    )
                    
                    # Đặt alias champion cho version tốt nhất (tính năng mới của MLflow)
                    client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
                    print(f"   => Thành công! Đã đăng ký Version mới: {mv.version} làm Champion.")
                else:
                    print("   -> KÉM HƠN HOẶC BẰNG! Bỏ qua đăng ký. (Chỉ lưu log experiment)")
            else:
                raise Exception("Chưa có model nào.")
                
        except Exception as e:
            print("   -> Chưa có mô hình nào trong Registry. Đang đăng ký model đầu tiên...")
            model_uri = f"runs:/{run_id}/model"
            mv = mlflow.register_model(model_uri, MODEL_NAME)
            
            # Thêm Description (mô tả) cho Model
            client.update_model_version(
                name=MODEL_NAME,
                version=mv.version,
                description="Mô hình ARIMA dự báo doanh thu dịch vụ 30 ngày tiếp theo. Dữ liệu huấn luyện từ ClickHouse `fact_service_revenue`."
            )
            
            client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
            print(f"   => Thành công! Đã đăng ký Version mới: {mv.version} làm Champion.")

if __name__ == "__main__":
    main()
