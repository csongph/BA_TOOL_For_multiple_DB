CREATE TABLE hrEmployee_Test_Error (
    -- 1. Datatype ที่ไม่ใช่มาตรฐานทั่วไป หรือเป็น Custom Type (น่าจะเกิด Error)
    CustomID GEOGRAPHY NULL, 
    
    -- 2. Datatype ที่เก่ามากหรือถูกยกเลิกไปแล้ว (ควรขึ้น Warning)
    OldDescription TEXT COLLATE Thai_CI_AS NULL, 
    
    -- 3. การกำหนด Precision ที่ผิดปกติ (เช่น decimal ที่สเกลมากกว่าพรีซิชัน)
    WrongSalary decimal(5, 10) NULL, 
    
    -- 4. สะกด Datatype ผิด (Syntax Error แน่นอน)
    ErrorField NVARCHRRR(255) NULL,
    
    -- 5. Datatype เฉพาะทางที่ระบบ Mapping อาจยังไม่รองรับ
    BinaryData VARBINARY(MAX) NULL,
    JsonConfig JSON NULL
    -- แก้จาก varchar เป็นคำมั่วๆ
    EmployeeID ERROR_TYPE_999 NOT NULL,
);