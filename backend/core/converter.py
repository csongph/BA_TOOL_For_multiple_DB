import re
# raw_type ที่ถือว่าเป็น "byte output" ใน Avro
_BYTE_RAW_TYPES = {"bytes", "byte", "byte[]", "binary"}

# logical_type ที่ OK เมื่อ raw='bytes' (Avro decimal stored as bytes, หรือ binary type ปกติ)
_BYTES_OK_LOGICAL = {"decimal", "bytes"}


class DataTypeConverter:
    def __init__(self, mapping: dict):
        """
        mapping: dict ที่โหลดจาก DB (อาจเป็น default mapping หรือ per-db-pair)
        เก็บเป็น default; convert() รับ override_mapping เพื่อใช้แทนได้
        """
        self.mapping = mapping

    def normalize(self, sql_type: str) -> str:
        t = sql_type.lower().strip()
        return re.split(r"[\(\s]", t)[0]

    def apply_precision(self, sql_type: str, base: str, final_type: str,
                        has_length: bool = False,
                        has_precision: bool = False,
                        has_scale: bool = False) -> str:
        """
        ต่อ (n) หรือ (p,s) กลับเข้า final_type จาก sql_type ต้นทาง
        base          = normalized base type เช่น 'varchar', 'money'
        has_length    = DB บอกว่า dest type ต้องการ length argument
        has_precision = DB บอกว่า dest type ต้องการ precision argument
        has_scale     = DB บอกว่า dest type ต้องการ scale argument
        """
        if base in ("money", "smallmoney", "timestamp", "rowversion"):
            return final_type

        if "(" in final_type:
            return final_type

        match = re.search(r"\(([^)]+)\)", sql_type)

        # hardcoded fallback สำหรับ source types ที่รู้จัก
        _NEEDS_PRECISION = {
            "decimal", "numeric",
            "varchar", "char",
            "nvarchar", "nchar",
            "binary", "varbinary",
        }

        # ใช้ flag จาก DB ก่อน (ครอบคลุม dest type ที่ไม่อยู่ใน hardcoded set)
        # fallback ไป hardcoded set ถ้าไม่มี flag
        needs_p = has_length or has_precision or base in _NEEDS_PRECISION

        if needs_p and match:
            return f"{final_type}({match.group(1)})"

        # decimal/numeric ที่ไม่มี precision ใน source → ใส่ default (18,0)
        if (has_precision or base in ("decimal", "numeric")) and not match:
            return f"{final_type}(18,0)"

        return final_type

    def convert(self, sql_type: str, override_mapping: dict | None = None) -> dict:
        """
        แปลง sql_type → result dict

        override_mapping: ถ้าส่งมา จะใช้แทน self.mapping
                          ใช้สำหรับ per-db-pair mapping

        [FIX] ลำดับการหา mapping key:
          1. exact key  — "tinyint(1)" ตรงตัว (กรณี DB เก็บ source_type พร้อม precision)
          2. base key   — "tinyint"    หลังตัด (...) และ whitespace ออก
        ป้องกัน miss เมื่อ DB มี row source_type='tinyint(1)' แต่ normalize() คืน 'tinyint'
        """
        mapping = override_mapping if override_mapping is not None else self.mapping
        base = self.normalize(sql_type)

        exact_key = sql_type.lower().strip().rstrip(",")
        data = mapping.get(exact_key) or mapping.get(base)

        if not data:
            return {
                "input": sql_type, "raw": None, "logical": None, "final": None,
                "standard_type": None,
                "status": "unknown", "reason": f"Type '{base}' not found in mapping",
                "byte_anomaly": False,
                "byte_anomaly_detail": None,
            }

        final_base = data.get("dest_final") or data.get("final")
        final = self.apply_precision(
            sql_type, base, final_base,
            has_length=bool(data.get("has_length")),
            has_precision=bool(data.get("has_precision")),
            has_scale=bool(data.get("has_scale")),
        )

        standard_type = data.get("final")

        # ── ตรวจ byte anomaly ────────────────────────────────────
        raw_type_lower = (data["raw"] or "").lower().strip()
        logical_lower = (data["logical"] or "").lower().strip()
        is_byte_output = raw_type_lower in _BYTE_RAW_TYPES
        byte_anomaly = is_byte_output and logical_lower not in _BYTES_OK_LOGICAL

        return {
            "input": sql_type,
            "raw": data["raw"],
            "logical": data["logical"],
            "standard_type": standard_type,
            "final": final,
            "status": "ok",
            "reason": None,
            "byte_anomaly": byte_anomaly,
            "byte_anomaly_detail": (
                f"คอลัมน์ถูกแปลงเป็น '{data['raw']}' "
                f"แต่ logical type เป็น '{data['logical']}' — "
                f"กรุณาตรวจสอบ mapping"
            ) if byte_anomaly else None,
        }