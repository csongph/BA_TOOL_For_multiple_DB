CREATE TABLE hrEmployee (
    EmployeeID   VARCHAR(36)   NOT NULL,
    FirstName    VARCHAR(100)  NOT NULL,
    LastName     VARCHAR(100)  NOT NULL,
    Email        VARCHAR(150)  NULL,
    Department   VARCHAR(100)  NULL,
    Position     VARCHAR(100)  NULL,
    HireDate     DATE          NULL,
    IsActive     BOOLEAN      NOT NULL DEFAULT TRUE,
    CreatedAt    TIMESTAMPTZ   NOT NULL DEFAULT NOW(),

    PRIMARY KEY (EmployeeID),
    CONSTRAINT uq_employee_email UNIQUE (Email)
);
CREATE TABLE hrPayroll_Details (
    PayrollID              UUID              NOT NULL DEFAULT gen_random_uuid(),
    EmployeeID             VARCHAR(36)       NOT NULL,
    BaseSalary             NUMERIC(19, 2)    NOT NULL DEFAULT 0.00,
    TaxRate                NUMERIC(5, 2)     NOT NULL DEFAULT 0.00,
    SocialSecurityFund     NUMERIC(18, 4)    NOT NULL DEFAULT 0.00,
    OT_Rate                NUMERIC(10, 2)    NOT NULL DEFAULT 0.00,
    PaymentMethod          INTEGER           NOT NULL,
    IsTaxExempt BOOLEAN NOT NULL DEFAULT FALSE,
    LastPaymentTimestamp   TIMESTAMPTZ       NULL     DEFAULT NOW(),
    PayrollNote            TEXT              NULL,

    PRIMARY KEY (PayrollID),
    CONSTRAINT uq_payroll_employee UNIQUE (EmployeeID),
    CONSTRAINT fk_payroll_employee
        FOREIGN KEY (EmployeeID) REFERENCES hrEmployee(EmployeeID)
        ON DELETE CASCADE
);
CREATE INDEX idx_payroll_employee  ON hrPayroll_Details(EmployeeID);
CREATE INDEX idx_payroll_timestamp ON hrPayroll_Details(LastPaymentTimestamp);