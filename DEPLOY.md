# 🚀 Deploy Guide — SQL File Converter Frontend → Vercel

## ไฟล์ที่ได้รับ

```
.github/
  workflows/
    deploy.yml        ← GitHub Actions CI/CD
vercel.json           ← Vercel project config
.env.example          ← template สำหรับ environment variables
.gitignore            ← ป้องกัน commit ไฟล์ลับ
```

---

## ขั้นตอน Setup (ครั้งแรกครั้งเดียว)

### 1 · สร้าง Vercel account + เชื่อม repo

1. ไปที่ [vercel.com](https://vercel.com) → Sign up ด้วย GitHub
2. **Add New Project** → เลือก repo ของคุณ
3. Vercel จะ detect เป็น Static Site โดยอัตโนมัติ
4. กด **Deploy** ครั้งแรกด้วยมือ (เพื่อสร้าง project)

### 2 · ได้ Vercel Token

1. [vercel.com/account/tokens](https://vercel.com/account/tokens) → **Create Token**
2. ตั้งชื่อ เช่น `github-actions`
3. Copy token ไว้

### 3 · เพิ่ม Secret ใน GitHub

ไปที่ `repo → Settings → Secrets → Actions → New repository secret`

| Name | Value |
|------|-------|
| `VERCEL_TOKEN` | token จากขั้นตอนที่ 2 |

### 4 · Copy ไฟล์เข้า repo

```bash
# จาก root ของ project
cp -r .github vercel.json .env.example .gitignore /path/to/your/repo/
```

### 5 · Link Vercel project (local, ครั้งแรก)

```bash
npm i -g vercel
vercel login
vercel link   # เลือก project ที่สร้างในขั้นตอน 1
```

ไฟล์ `.vercel/project.json` จะถูกสร้าง — **ห้าม commit** (อยู่ใน .gitignore แล้ว)

---

## การใช้งานหลัง Setup

| Action | ผลลัพธ์ |
|--------|---------|
| `git push origin main` | Deploy production โดยอัตโนมัติ |
| เปิด Pull Request | สร้าง Preview URL พร้อม comment ใน PR |
| Merge PR → main | Deploy production |

---

## ตั้งค่า API_BASE สำหรับ Production

เมื่อ backend พร้อม ให้ตั้งค่าใน Vercel Dashboard:

1. Project → **Settings** → **Environment Variables**
2. เพิ่ม `API_BASE` = URL ของ backend จริง
3. กด **Redeploy**

หรือแก้ใน `index.html` ตรง:
```html
<script>
  window.API_BASE = 'https://your-api.example.com';  // ← เปลี่ยนตรงนี้
</script>
```

---

## โครงสร้าง Deploy Flow

```
git push → GitHub Actions
              ├── push to main  → vercel deploy --prod  → 🌐 Production
              └── pull_request  → vercel deploy          → 🔍 Preview URL
                                                            └── comment ใน PR
```

---

## Checklist ก่อน Go Live

- [ ] `vercel link` รันแล้วใน local
- [ ] `VERCEL_TOKEN` อยู่ใน GitHub Secrets
- [ ] `.vercel/` อยู่ใน .gitignore
- [ ] `.env` ไม่ถูก commit
- [ ] ทดสอบ push ไปที่ branch อื่น (ไม่ใช่ main) เพื่อดู Preview URL
