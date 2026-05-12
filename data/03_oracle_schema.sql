CREATE TABLE hrEmployee (
    EmployeeID   VARCHAR2(36)   NOT NULL,
    FirstName    VARCHAR2(100)  NOT NULL,
    LastName     VARCHAR2(100)  NOT NULL,
    Email        VARCHAR2(150)  NULL,
    Department   VARCHAR2(100)  NULL,
    Position     VARCHAR2(100)  NULL,
    HireDate     DATE           NULL,
    IsActive     NUMBER(1)      NOT NULL DEFAULT 1,

    CreatedAt    TIMESTAMP WITH TIME ZONE  NOT NULL DEFAULT SYSTIMESTAMP,

    CONSTRAINT pk_employee PRIMARY KEY (EmployeeID),
    CONSTRAINT uq_employee_email UNIQUE (Email)
);

CREATE INDEX idx_employee_id ON hrEmployee(EmployeeID);
CREATE TABLE hrPayroll_Details (
    PayrollID              VARCHAR2(36)          NOT NULL,
    EmployeeID             VARCHAR2(36)          NOT NULL,
    BaseSalary             NUMBER(19, 2)         NOT NULL DEFAULT 0,
    TaxRate                NUMBER(5, 2)          NOT NULL DEFAULT 0,
    SocialSecurityFund     NUMBER(18, 4)         NOT NULL DEFAULT 0,
    OT_Rate                NUMBER(10, 2)         NOT NULL DEFAULT 0,
    PaymentMethod          NUMBER(10)            NOT NULL,
    IsTaxExempt            NUMBER(1)             NOT NULL DEFAULT 0,
    LastPaymentTimestamp   TIMESTAMP WITH TIME ZONE  NULL DEFAULT SYSTIMESTAMP,
    PayrollNote            CLOB                  NULL,

    CONSTRAINT pk_payroll PRIMARY KEY (PayrollID),
    CONSTRAINT uq_payroll_employee UNIQUE (EmployeeID),
    CONSTRAINT fk_payroll_employee
        FOREIGN KEY (EmployeeID) REFERENCES hrEmployee(EmployeeID)
        ON DELETE CASCADE
        -- Oracle ไม่รองรับ ON UPDATE CASCADE — ถูกต้องที่ไม่มี
);
CREATE OR REPLACE TRIGGER trg_payroll_id
    BEFORE INSERT ON hrPayroll_Details
    FOR EACH ROW
BEGIN
    IF :NEW.PayrollID IS NULL THEN
        -- แปลง RAW(16) จาก SYS_GUID() เป็น UUID string format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
        :NEW.PayrollID := LOWER(
            REGEXP_REPLACE(RAWTOHEX(SYS_GUID()),
                '([A-F0-9]{8})([A-F0-9]{4})([A-F0-9]{4})([A-F0-9]{4})([A-F0-9]{12})',
                '\1-\2-\3-\4-\5')
        );
    END IF;
END;
CREATE INDEX idx_payroll_employee  ON hrPayroll_Details(EmployeeID);
CREATE INDEX idx_payroll_timestamp ON hrPayroll_Details(LastPaymentTimestamp);