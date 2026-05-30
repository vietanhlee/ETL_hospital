import os
import sys
import codecs
import pandas as pd
import numpy as np

# Fix lỗi Unicode trên terminal Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

import clickhouse_connect
import mlflow
import mlflow.statsmodels
from mlflow.models import infer_signature
from statsmodels.tsa.statespace.sarimax import SARIMAX
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
from mlflow.tracking import MlflowClient

import warnings
from statsmodels.tools.sm_exceptions import ConvergenceWarning
warnings.simplefilter('ignore', ConvergenceWarning)

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
MODEL_NAME = "SARIMA_Revenue_Model"


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
    # Xử lý missing dates nếu có bằng nội suy tuyến tính (Linear Interpolation)
    df = df.asfreq('D')
    df['daily_revenue'] = df['daily_revenue'].interpolate(method='linear')
    
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
        import itertools
        
        print("   -> Đang chạy Grid Search tìm siêu tham số tốt nhất (vui lòng chờ)...")
        # Định nghĩa không gian tìm kiếm tham số (giới hạn 0-1 để chạy nhanh)
        p = d = q = range(0, 2)
        pdq = list(itertools.product(p, d, q))
        seasonal_pdq = [(x[0], x[1], x[2], 7) for x in list(itertools.product(p, d, q))] # Mùa vụ 7 ngày
        
        print(f"   -> Đang quét tổng cộng {len(pdq) * len(seasonal_pdq)} mô hình để tìm AIC thấp nhất...")
        
        best_aic = float("inf")
        best_model = None
        best_param = None
        best_param_seasonal = None
        
        for param in pdq:
            for param_seasonal in seasonal_pdq:
                try:
                    mod = SARIMAX(train_data['daily_revenue'],
                                  order=param,
                                  seasonal_order=param_seasonal,
                                  enforce_stationarity=False,
                                  enforce_invertibility=False)
                    results = mod.fit(disp=False)
                    
                    # Chọn mô hình có chỉ số AIC thấp nhất
                    if results.aic < best_aic:
                        best_aic = results.aic
                        best_model = results
                        best_param = param
                        best_param_seasonal = param_seasonal
                except:
                    continue
                    
        fitted_model = best_model
        p, d, q = best_param
        P, D, Q, s = best_param_seasonal
        
        print(f"   -> Cấu hình tối ưu: Order={best_param}, Seasonal={best_param_seasonal}, AIC={best_aic:,.2f}")
        
        # Dự báo trên tập test (30 ngày) và tương lai (30 ngày tiếp theo)
        future_steps = 30
        total_steps = test_size + future_steps
        all_predictions = fitted_model.forecast(steps=total_steps)
        
        # Tách ra: 30 ngày cho test, 30 ngày cho tương lai
        predictions = all_predictions.iloc[:test_size]
        future_predictions = all_predictions.iloc[test_size:]
        
        # Đánh giá độ chính xác (Metrics) trên tập test
        mae = mean_absolute_error(test_data['daily_revenue'], predictions)
        rmse = np.sqrt(mean_squared_error(test_data['daily_revenue'], predictions))
        
        print(f"   -> Kết quả Test: MAE = {mae:,.2f} | RMSE = {rmse:,.2f}")
        
        # Log tham số và kết quả vào MLflow
        mlflow.log_param("p", p)
        mlflow.log_param("d", d)
        mlflow.log_param("q", q)
        mlflow.log_param("P_seasonal", P)
        mlflow.log_param("D_seasonal", D)
        mlflow.log_param("Q_seasonal", Q)
        mlflow.log_param("s_seasonality", s)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        
        # Định dạng tiền tệ cho MAE và RMSE (Million / Billion)
        def format_currency(val):
            if val >= 1e9:
                return f"{val / 1e9:.2f}B"
            elif val >= 1e6:
                return f"{val / 1e6:.2f}M"
            else:
                return f"{val:,.0f}"
                
        mae_str = format_currency(mae)
        rmse_str = format_currency(rmse)
        
        # Vẽ biểu đồ dự báo vs thực tế và tương lai
        import matplotlib.ticker as ticker
        fig, ax = plt.subplots(figsize=(14, 7))
        
        # Bỏ đường train, chỉ vẽ test thực tế và tương lai
        ax.plot(test_data.index, test_data['daily_revenue'], label="Doanh thu thực tế (Test)", color="#1f77b4", linewidth=2, marker='o', markersize=4)
        ax.plot(test_data.index, predictions, label="Dự báo SARIMA (Test)", color="#ff7f0e", linestyle="--", linewidth=2)
        
        # Vẽ phần tương lai (màu xanh lá)
        ax.plot(future_predictions.index, future_predictions, label="Dự báo tương lai (30 ngày)", color="#2ca02c", linestyle="-", linewidth=2, marker='^', markersize=5)
        
        # Làm đẹp biểu đồ: Thêm lưới, nhãn và định dạng
        ax.grid(True, linestyle='--', alpha=0.6)
        ax.set_title(f"Biểu đồ Dự báo Doanh thu SARIMA ({future_steps} ngày tới)", fontsize=16, fontweight='bold', pad=15)
        ax.set_xlabel("Thời gian (Ngày / Tháng)", fontsize=12, labelpad=10)
        ax.set_ylabel("Doanh thu (VNĐ)", fontsize=12, labelpad=10)
        
        # In các chỉ số MAE, RMSE lên góc trái biểu đồ
        textstr = f"Sai số mô hình (Test 30 ngày):\nMAE:  {mae_str} VNĐ\nRMSE: {rmse_str} VNĐ"
        props = dict(boxstyle='round', facecolor='#e6f2ff', edgecolor='#1f77b4', alpha=0.8)
        ax.text(0.02, 0.95, textstr, transform=ax.transAxes, fontsize=12,
                verticalalignment='top', bbox=props, fontweight='bold', color='#003366')
        
        # Định dạng số tiền trục Y có dấu phẩy (VD: 10,000,000)
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
        
        ax.legend(fontsize=11, loc="best")
        fig.tight_layout()
        
        # Log biểu đồ vào MLflow
        mlflow.log_figure(fig, "forecast_plot.png")
        plt.close(fig)
        
        
        try:
            mlflow.statsmodels.log_model(
                statsmodels_model=fitted_model,
                artifact_path="model"
            )
            print("   -> Log model artifact thành công!")
        except Exception as e:
            print(f"   -> Lỗi khi log model: {e}")
        
        run_id = run.info.run_id
        
        # ==========================================
        # MLOPS: SO SÁNH & REGISTER MODEL (CI/CD cho Model)
        # ==========================================
        print("4. Đang kiểm tra Model Registry để so sánh độ chính xác...")
        client = MlflowClient()
        
        try:
            # Lấy version của model đang được gắn alias "champion"
            champion_version = client.get_model_version_by_alias(MODEL_NAME, "champion")
            run_id_of_champion = champion_version.run_id
            
            # Lấy MAE của model champion từ trong lịch sử MLflow
            old_metrics = client.get_run(run_id_of_champion).data.metrics
            old_mae = old_metrics.get("mae", float('inf'))
            
            print(f"   -> Model Champion hiện tại (Version {champion_version.version}) có MAE: {old_mae:,.2f}")
            
            if mae <= old_mae:
                print("   -> TỐT HƠN! Mô hình mới có sai số (MAE) nhỏ hơn Champion. Đang tiến hành Register...")
                # Register model mới
                model_uri = f"runs:/{run_id}/model"
                mv = mlflow.register_model(model_uri, MODEL_NAME)
                
                # Thêm Description (mô tả) cho Model
                client.update_model_version(
                    name=MODEL_NAME,
                    version=mv.version,
                    description="Mô hình SARIMA dự báo doanh thu dịch vụ 30 ngày tiếp theo. Dữ liệu huấn luyện từ ClickHouse `fact_service_revenue`."
                )
                
                # Đặt alias champion cho version tốt nhất (tính năng mới của MLflow)
                client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
                print(f"   => Thành công! Đã đăng ký Version mới: {mv.version} làm Champion.")
            else:
                print("   -> KÉM HƠN HOẶC BẰNG! Bỏ qua đăng ký. (Chỉ lưu log experiment)")
                
        except Exception as e:
            print("   -> Chưa có mô hình nào trong Registry. Đang đăng ký model đầu tiên...")
            model_uri = f"runs:/{run_id}/model"
            mv = mlflow.register_model(model_uri, MODEL_NAME)
            
            # Thêm Description (mô tả) cho Model
            client.update_model_version(
                name=MODEL_NAME,
                version=mv.version,
                description="Mô hình SARIMA dự báo doanh thu dịch vụ 30 ngày tiếp theo. Dữ liệu huấn luyện từ ClickHouse `fact_service_revenue`."
            )
            
            client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
            print(f"   => Thành công! Đã đăng ký Version mới: {mv.version} làm Champion.")

if __name__ == "__main__":
    main()
