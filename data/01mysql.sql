CREATE DATABASE IF NOT EXISTS hr_system CHARACTER SET utf8mb4 COLLATE utf8mb4_thai_ci;
USE hr_system;
CREATE TABLE hrEmployee (
    EmployeeID   CHAR(36)      NOT NULL,
    FirstName    VARCHAR(100)  NOT NULL,
    LastName     VARCHAR(100)  NOT NULL,
    Email        VARCHAR(150)  NULL,
    Department   VARCHAR(100)  NULL,
    Position     VARCHAR(100)  NULL,
    HireDate     DATE          NULL,
    IsActive     TINYINT(1)    NOT NULL DEFAULT 1,
    CreatedAt    TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (EmployeeID),
    UNIQUE INDEX idx_unique_email (Email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_thai_ci;
CREATE TABLE hrPayroll_Details (
    PayrollID            CHAR(36)       NOT NULL DEFAULT (UUID()),
    EmployeeID           CHAR(36)       NOT NULL,
    BaseSalary           DECIMAL(19, 2) NOT NULL DEFAULT 0.00,
    TaxRate              DECIMAL(5, 2)  NOT NULL DEFAULT 0.00,
    SocialSecurityFund   DECIMAL(18, 4) NOT NULL DEFAULT 0.00,
    OT_Rate              DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    PaymentMethod        INT            NOT NULL COMMENT '1: Cash, 2: Transfer',
    IsTaxExempt          TINYINT(1)     NOT NULL DEFAULT 0,
    LastPaymentTimestamp TIMESTAMP      NULL     DEFAULT CURRENT_TIMESTAMP
                                                 ON UPDATE CURRENT_TIMESTAMP
                                                 COMMENT 'Stored as UTC internally',
    PayrollNote          TEXT           NULL,

    PRIMARY KEY (PayrollID),
    UNIQUE INDEX uq_employee (EmployeeID),
    CONSTRAINT fk_payroll_employee
        FOREIGN KEY (EmployeeID) REFERENCES hrEmployee(EmployeeID)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_thai_ci;