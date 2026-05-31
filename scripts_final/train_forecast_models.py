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
from statsmodels.tsa.statespace.sarimax import SARIMAX
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
from mlflow.tracking import MlflowClient

import warnings
from statsmodels.tools.sm_exceptions import ConvergenceWarning
warnings.simplefilter('ignore', ConvergenceWarning)
warnings.filterwarnings('ignore', message='Too few observations to estimate starting parameters')
warnings.filterwarnings('ignore', category=RuntimeWarning)

# ==========================================
# CẤU HÌNH KẾT NỐI
# ==========================================
CH_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')   # fallback về container local
CH_PORT = int(os.getenv('CLICKHOUSE_PORT', '8123'))     # HTTP port local (không SSL)
CH_USER = os.getenv('CLICKHOUSE_USER', 'default')
CH_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', '')      # local không có password
CH_DB = 'hospital_dw'
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

def get_data_from_clickhouse(forecast_type):
    """
    Kết nối ClickHouse, lấy dữ liệu (doanh thu hoặc số lượng người dùng) theo ngày.
    """
    print(f"1. Đang kết nối tới ClickHouse để truy xuất dữ liệu [{forecast_type}]...")
    client = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD, secure=True
    )
    
    if forecast_type == "revenue":
        query = """
        SELECT DateKey, SUM(RevenueAmount) as target_val
        FROM hospital_dw.fact_service_revenue
        GROUP BY DateKey ORDER BY DateKey ASC
        """
    else: # user_count
        query = """
        SELECT DateKey, COUNT(DISTINCT PatientKey) as target_val
        FROM hospital_dw.fact_service_revenue
        GROUP BY DateKey ORDER BY DateKey ASC
        """
        
    df = client.query_df(query)
    if df.empty:
        raise ValueError(f"Không có dữ liệu trong bảng fact_service_revenue cho {forecast_type}!")
    
    df['Date'] = pd.to_datetime(df['DateKey'].astype(str), format='%Y%m%d')
    df = df.sort_values('Date').reset_index(drop=True)
    df.set_index('Date', inplace=True)
    
    df = df[['target_val']]
    df['target_val'] = pd.to_numeric(df['target_val'], errors='coerce')
    df = df.asfreq('D')
    df['target_val'] = df['target_val'].interpolate(method='linear')
    if forecast_type == "user_count":
        df['target_val'] = df['target_val'].round()
        
    return df


def train_and_register_model(forecast_type):
    if forecast_type == "revenue":
        EXPERIMENT_NAME = "Revenue_Forecasting"
        MODEL_NAME = "SARIMA_Revenue_Model"
        desc_text = "Mô hình SARIMA dự báo doanh thu dịch vụ 30 ngày tiếp theo. Dữ liệu huấn luyện từ ClickHouse `fact_service_revenue`."
        ylabel = "Doanh thu (VNĐ)"
        title = "Doanh thu"
        unit = "VNĐ"
    else:
        EXPERIMENT_NAME = "User_Count_Forecasting"
        MODEL_NAME = "SARIMA_User_Count_Model"
        desc_text = "Mô hình SARIMA dự báo số lượng người sử dụng dịch vụ 30 ngày tiếp theo. Dữ liệu huấn luyện: số bệnh nhân duy nhất (PatientKey) từ `fact_service_revenue`."
        ylabel = "Số lượng người"
        title = "Lượt Người sử dụng Dịch vụ"
        unit = "người"
        
    print(f"\n=======================================================")
    print(f"BẮT ĐẦU TRAINING VÀ CI/CD CHO: {EXPERIMENT_NAME}")
    print(f"=======================================================")
    
    df = get_data_from_clickhouse(forecast_type)
    print(f"   Lấy thành công {len(df)} ngày dữ liệu lịch sử.")
    
    val_size = 15
    test_size = 15
    total_eval_size = val_size + test_size
    if len(df) <= total_eval_size:
        raise ValueError("Dữ liệu quá ít để chia tập (cần > 30 ngày).")
        
    train_data = df.iloc[:-total_eval_size]
    val_data = df.iloc[-total_eval_size:-test_size]
    test_data = df.iloc[-test_size:]
    print(f"2. Đã chia dữ liệu: Train ({len(train_data)} ngày) - Val ({len(val_data)} ngày) - Test ({len(test_data)} ngày)")
    
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    print(f"3. Bắt đầu quá trình Train và Log vào MLflow ({MLFLOW_TRACKING_URI})...")
    
    with mlflow.start_run(log_system_metrics=False) as run:
        import itertools
        print("   -> Đang chạy Grid Search tìm siêu tham số tốt nhất (vui lòng chờ)...")
        p = d = q = range(0, 3)
        pdq = list(itertools.product(p, d, q))
        seasonal_pdq = [(x[0], x[1], x[2], 7) for x in list(itertools.product(p, d, q))]
        
        print(f"   -> Đang quét tổng cộng {len(pdq) * len(seasonal_pdq)} mô hình để tìm Validation MAE thấp nhất...")
        best_mae = float("inf")
        best_model = None
        best_param = None
        best_param_seasonal = None
        
        for param in pdq:
            for param_seasonal in seasonal_pdq:
                try:
                    mod = SARIMAX(train_data['target_val'],
                                  order=param,
                                  seasonal_order=param_seasonal,
                                  enforce_stationarity=False,
                                  enforce_invertibility=False)
                    results = mod.fit(disp=False)
                    
                    # Đánh giá bằng MAE dự báo trên tập Validation
                    preds = results.forecast(steps=val_size)
                    val_mae = mean_absolute_error(val_data['target_val'], preds)
                    
                    if val_mae < best_mae:
                        best_mae = val_mae
                        best_model = results
                        best_param = param
                        best_param_seasonal = param_seasonal
                except:
                    continue
                    
        print(f"   -> Cấu hình tối ưu: Order={best_param}, Seasonal={best_param_seasonal}, Validation MAE={best_mae:,.2f}")
        
        print(f"   -> Đang re-fit mô hình trên tập (Train + Validation) để dự báo Test...")
        train_val_data = pd.concat([train_data, val_data])
        mod_final = SARIMAX(train_val_data['target_val'],
                            order=best_param,
                            seasonal_order=best_param_seasonal,
                            enforce_stationarity=False,
                            enforce_invertibility=False)
        fitted_model = mod_final.fit(disp=False)
        
        future_steps = 30
        total_steps = test_size + future_steps
        all_predictions = fitted_model.forecast(steps=total_steps)
        predictions = all_predictions.iloc[:test_size]
        future_predictions = all_predictions.iloc[test_size:]
        
        mae = mean_absolute_error(test_data['target_val'], predictions)
        rmse = np.sqrt(mean_squared_error(test_data['target_val'], predictions))
        print(f"   -> Kết quả Test ({test_size} ngày): MAE = {mae:,.2f} | RMSE = {rmse:,.2f}")
        
        mlflow.log_param("p", best_param[0])
        mlflow.log_param("d", best_param[1])
        mlflow.log_param("q", best_param[2])
        mlflow.log_param("P_seasonal", best_param_seasonal[0])
        mlflow.log_param("D_seasonal", best_param_seasonal[1])
        mlflow.log_param("Q_seasonal", best_param_seasonal[2])
        mlflow.log_param("s_seasonality", best_param_seasonal[3])
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("rmse", rmse)
        
        def format_val(val):
            if forecast_type == "revenue":
                if val >= 1e9: return f"{val / 1e9:.2f}B"
                elif val >= 1e6: return f"{val / 1e6:.2f}M"
            return f"{val:,.0f}" if forecast_type == "revenue" else f"{val:,.2f}"

        mae_str = format_val(mae)
        rmse_str = format_val(rmse)
        
        import matplotlib.ticker as ticker
        fig, ax = plt.subplots(figsize=(14, 7))
        ax.plot(test_data.index, test_data['target_val'], label=f"Thực tế (Test)", color="#1f77b4", linewidth=2, marker='o', markersize=4)
        ax.plot(test_data.index, predictions, label="Dự báo SARIMA (Test)", color="#ff7f0e", linestyle="--", linewidth=2)
        ax.plot(future_predictions.index, future_predictions, label="Dự báo tương lai (30 ngày)", color="#2ca02c", linestyle="-", linewidth=2, marker='^', markersize=5)
        
        ax.grid(True, linestyle='--', alpha=0.6)
        ax.set_title(f"Biểu đồ Dự báo {title} SARIMA ({future_steps} ngày tới)", fontsize=16, fontweight='bold', pad=15)
        ax.set_xlabel("Thời gian (Ngày / Tháng)", fontsize=12, labelpad=10)
        ax.set_ylabel(ylabel, fontsize=12, labelpad=10)
        
        textstr = f"Sai số mô hình (Test {test_size} ngày):\nMAE:  {mae_str} {unit}\nRMSE: {rmse_str} {unit}"
        props = dict(boxstyle='round', facecolor='#e6f2ff', edgecolor='#1f77b4', alpha=0.8)
        ax.text(0.02, 0.95, textstr, transform=ax.transAxes, fontsize=12,
                verticalalignment='top', bbox=props, fontweight='bold', color='#003366')
        
        ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
        ax.legend(fontsize=11, loc="best")
        fig.tight_layout()
        
        mlflow.log_figure(fig, f"forecast_plot_{forecast_type}.png")
        plt.close(fig)
        
        try:
            mlflow.statsmodels.log_model(statsmodels_model=fitted_model, name="model")
            print("   -> Log model artifact thành công!")
        except Exception as e:
            print(f"   -> Lỗi khi log model: {e}")
            
        run_id = run.info.run_id
        
        print("4. Đang kiểm tra Model Registry để so sánh độ chính xác...")
        client = MlflowClient()
        
        try:
            champion_version = client.get_model_version_by_alias(MODEL_NAME, "champion")
            print(f"   -> Đang tải model Champion (Version {champion_version.version}) để dự báo trên tập test hiện tại...")
            champion_model = mlflow.statsmodels.load_model(f"models:/{MODEL_NAME}@champion")
            
            champion_predictions = champion_model.predict(start=test_data.index[0], end=test_data.index[-1])
            old_mae = mean_absolute_error(test_data['target_val'], champion_predictions)
            
            print(f"   -> Model Champion (Version {champion_version.version}) trên tập test hiện tại có MAE: {old_mae:,.2f}")
            print(f"   -> Model mới train có MAE: {mae:,.2f}")
            
            if mae <= old_mae:
                print("   -> TỐT HƠN HOẶC BẰNG! Đang tiến hành Register...")
                mv = mlflow.register_model(f"runs:/{run_id}/model", MODEL_NAME)
                client.update_model_version(name=MODEL_NAME, version=mv.version, description=desc_text)
                client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
                print(f"   => Thành công! Đã đăng ký Version mới: {mv.version} làm Champion.")
            else:
                print("   -> KÉM HƠN HOẶC BẰNG! Bỏ qua đăng ký. (Chỉ lưu log experiment)")
                
        except Exception as e:
            print(f"   -> Chưa có mô hình Champion hoặc có lỗi khi tải ({e}). Đang đăng ký model này làm Champion...")
            mv = mlflow.register_model(f"runs:/{run_id}/model", MODEL_NAME)
            client.update_model_version(name=MODEL_NAME, version=mv.version, description=desc_text)
            client.set_registered_model_alias(MODEL_NAME, "champion", mv.version)
            print(f"   => Thành công! Đã đăng ký Version mới: {mv.version} làm Champion.")

import concurrent.futures

def main():
    forecast_types = ["revenue", "user_count"]
    print(f"🚀 Bắt đầu huấn luyện ĐA TIẾN TRÌNH cho {len(forecast_types)} mô hình...")
    
    # Khởi tạo ProcessPoolExecutor với 2 worker chạy song song
    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as executor:
        # Đẩy các tác vụ vào pool
        futures = {executor.submit(train_and_register_model, ftype): ftype for ftype in forecast_types}
        
        # Chờ và lấy kết quả của từng tiến trình khi nó hoàn thành
        for future in concurrent.futures.as_completed(futures):
            ftype = futures[future]
            try:
                future.result() # Bắt lỗi nếu tiến trình ném ra Exception
                print(f"✅ Đã hoàn thành toàn bộ Pipeline cho mô hình: {ftype.upper()}")
            except Exception as exc:
                print(f"❌ Lỗi nghiêm trọng khi huấn luyện mô hình {ftype.upper()}: {exc}")

if __name__ == "__main__":
    main()
