-- Database creation (Bỏ comment nếu muốn tạo luôn database)
-- CREATE DATABASE IF NOT EXISTS hospital_dw;
-- USE hospital_dw;

-- ==========================================
-- DIMENSION TABLES
-- ==========================================

CREATE TABLE IF NOT EXISTS dim_date (
    DateKey Int32, 
    FullDate Date, 
    DayNumber Int32, 
    MonthNumber Int32, 
    MonthName String, 
    QuarterNumber Int32, 
    YearNumber Int32, 
    DayOfWeekNumber Int32, 
    DayOfWeekName String, 
    IsWeekend Int32
) ENGINE = ReplacingMergeTree() ORDER BY (DateKey);

CREATE TABLE IF NOT EXISTS dim_patient (
    PatientKey Int64, 
    PatientCode String, 
    FullName String, 
    Gender String, 
    BirthYear Int32, 
    AgeGroup String, 
    City String
) ENGINE = ReplacingMergeTree() ORDER BY (PatientCode);

CREATE TABLE IF NOT EXISTS dim_doctor (
    DoctorKey Int64, 
    DoctorCode String, 
    DoctorName String, 
    Gender String, 
    Specialty String, 
    AcademicTitle String
) ENGINE = ReplacingMergeTree() ORDER BY (DoctorCode);

CREATE TABLE IF NOT EXISTS dim_department (
    DepartmentKey Int64, 
    DepartmentCode String, 
    DepartmentName String, 
    DepartmentGroup String
) ENGINE = ReplacingMergeTree() ORDER BY (DepartmentCode);

CREATE TABLE IF NOT EXISTS dim_service (
    ServiceKey Int64, 
    ServiceCode String, 
    ServiceName String, 
    ServiceType String
) ENGINE = ReplacingMergeTree() ORDER BY (ServiceCode);

CREATE TABLE IF NOT EXISTS dim_medicine (
    MedicineKey Int64, 
    MedicineCode String, 
    MedicineName String, 
    MedicineGroup String, 
    Unit String
) ENGINE = ReplacingMergeTree() ORDER BY (MedicineCode);

CREATE TABLE IF NOT EXISTS dim_insurance (
    InsuranceKey Int64, 
    InsuranceCode String, 
    InsuranceType String, 
    CoverageRate Float64
) ENGINE = ReplacingMergeTree() ORDER BY (InsuranceCode);

-- ==========================================
-- FACT TABLES
-- ==========================================

CREATE TABLE IF NOT EXISTS fact_visit (
    VisitFactKey Int64, 
    DateKey Int32, 
    PatientKey Int64, 
    DoctorKey Int64, 
    DepartmentKey Int64, 
    InsuranceKey Int64, 
    VisitCode String, 
    ConsultationFee Decimal(18,2), 
    WaitingMinutes Int32, 
    VisitCount Int32
) ENGINE = ReplacingMergeTree() ORDER BY (VisitCode);

CREATE TABLE IF NOT EXISTS fact_service_revenue (
    ServiceRevenueFactKey Int64, 
    DateKey Int32, 
    PatientKey Int64, 
    DoctorKey Int64, 
    DepartmentKey Int64, 
    ServiceKey Int64, 
    InsuranceKey Int64, 
    VisitCode String, 
    ServiceTransactionCode String, 
    Quantity Int32, 
    UnitPrice Decimal(18,2), 
    RevenueAmount Decimal(18,2), 
    InsurancePaidAmount Decimal(18,2), 
    PatientPaidAmount Decimal(18,2)
) ENGINE = ReplacingMergeTree() ORDER BY (ServiceTransactionCode);

CREATE TABLE IF NOT EXISTS fact_prescription (
    PrescriptionFactKey Int64, 
    DateKey Int32, 
    PatientKey Int64, 
    DoctorKey Int64, 
    DepartmentKey Int64, 
    MedicineKey Int64, 
    InsuranceKey Int64, 
    VisitCode String, 
    PrescriptionCode String, 
    Quantity Int32, 
    UnitPrice Decimal(18,2), 
    MedicineRevenueAmount Decimal(18,2), 
    InsurancePaidAmount Decimal(18,2), 
    PatientPaidAmount Decimal(18,2)
) ENGINE = ReplacingMergeTree() ORDER BY (PrescriptionCode);
