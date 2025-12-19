# Laporan UAS Sistem Terdistribusi
## Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi

**Nama**: Reno Fahrezi Purnomo  
**NIM**: 11221051  
**Mata Kuliah**: Sistem Terdistribusi  
**Semester**: Ganjil 2024/2025

---

## Daftar Isi

1. [Ringkasan Sistem](#ringkasan-sistem)
2. [Bagian Teori (T1-T10)](#bagian-teori)
3. [Arsitektur dan Implementasi](#arsitektur-dan-implementasi)
4. [Analisis Performa](#analisis-performa)
5. [Kesimpulan](#kesimpulan)
6. [Referensi](#referensi)

---

## Ringkasan Sistem

Sistem yang dibangun adalah Pub-Sub log aggregator terdistribusi yang berjalan menggunakan Docker Compose. Sistem ini terdiri dari empat layanan utama:

1. **Aggregator (FastAPI)**: Layanan utama yang menyediakan REST API untuk menerima dan mengakses event, serta menjalankan 3 consumer workers untuk memproses event dari Redis queue secara paralel.

2. **Publisher**: Generator event yang menghasilkan log events dengan tingkat duplikasi 35% (7,000 dari 20,000 events), bertujuan menguji mekanisme deduplikasi.

3. **Broker (Redis)**: Message broker menggunakan Redis List sebagai queue untuk komunikasi asinkron antara publisher dan consumer workers.

4. **Storage (PostgreSQL)**: Database persisten untuk menyimpan events dan statistik dengan unique constraint pada `(topic, event_id)` untuk deduplication atomik.

**Fitur utama sistem:**
- **Idempotent Consumer**: Event dengan pasangan `(topic, event_id)` yang sama hanya diproses sekali
- **Strong Deduplication**: Menggunakan `INSERT ... ON CONFLICT DO NOTHING` di PostgreSQL
- **Transaksi Atomic**: Semua operasi dalam satu transaksi untuk konsistensi data
- **Kontrol Konkurensi**: Multi-worker dengan isolation level READ COMMITTED
- **Persistensi**: Docker named volumes memastikan data aman meski container dihapus

---

## Bagian Teori

### T1 (Bab 1): Karakteristik Sistem Terdistribusi dan Trade-off Desain

Sistem terdistribusi memiliki karakteristik utama: (1) concurrency - komponen berjalan paralel, (2) lack of global clock - tidak ada waktu global yang sinkron, (3) independent failures - komponen dapat gagal secara independen (Tanenbaum & Van Steen, 2017, hlm. 2-4).

Pada Pub-Sub Log Aggregator ini, trade-off desain yang dihadapi:

**Consistency vs Availability**: Sistem memilih consistency dengan menggunakan transaksi PostgreSQL dan unique constraints. Ketika database tidak tersedia, operasi akan gagal daripada menerima data yang mungkin duplikat.

**Throughput vs Latency**: Dengan menggunakan Redis queue dan 3 async consumer workers, sistem mengoptimalkan throughput. Event tidak langsung diproses tapi masuk queue terlebih dahulu, mengorbankan sedikit latency untuk throughput yang lebih tinggi (~95 events/sec).

**Simplicity vs Fault Tolerance**: Redis sebagai broker menambah kompleksitas namun meningkatkan fault tolerance karena publisher dan consumer terdecouple.

---

### T2 (Bab 2): Kapan Memilih Arsitektur Publish-Subscribe Dibanding Client-Server

Arsitektur publish-subscribe lebih tepat dipilih dalam kondisi berikut (Tanenbaum & Van Steen, 2017, hlm. 45-50):

**1. Decoupling Requirement**: Publisher yang generate log tidak perlu mengetahui siapa consumer-nya. Dalam implementasi ini, Publisher service hanya tahu Redis URL, tidak perlu tahu detail Aggregator.

**2. One-to-Many Communication**: Satu event bisa diterima oleh multiple consumers. Meski saat ini hanya ada 3 workers, arsitektur ini memungkinkan scaling horizontal dengan mudah.

**3. Asynchronous Processing**: Publisher tidak menunggu response pemrosesan. Setelah event masuk Redis queue, publisher lanjut ke event berikutnya. Ini terlihat dari send rate yang konsisten (~95 events/sec).

**4. Load Buffering**: Redis queue bertindak sebagai buffer yang menyerap spike traffic. Ketika publisher mengirim 20,000 events, consumer workers memproses secara bertahap tanpa overload.

---

### T3 (Bab 3): At-Least-Once vs Exactly-Once Delivery; Peran Idempotent Consumer

**At-Least-Once Delivery**: Message dijamin terkirim minimal sekali, tapi bisa lebih. Publisher akan retry jika tidak mendapat acknowledgment. Lebih mudah diimplementasi tapi bisa menyebabkan duplikasi.

**Exactly-Once Delivery**: Message terkirim tepat satu kali. Ideal tapi sangat sulit dicapai karena membutuhkan two-phase commit yang mahal.

**Peran Idempotent Consumer**: Solusi pragmatis yang mengombinasikan at-least-once delivery dengan deduplikasi di sisi consumer. Dalam implementasi ini:

```sql
INSERT INTO processed_events (topic, event_id, processed_at)
VALUES ($1, $2, NOW())
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id
```

Consumer menerima pesan yang mungkin duplikat (at-least-once), tapi database constraint memastikan hanya satu yang tersimpan. Hasil pengujian menunjukkan dari 20,001 events yang diterima, 7,000 terdeteksi sebagai duplikat dan di-drop, memberikan semantik exactly-once processing yang efektif.

---

### T4 (Bab 4): Skema Penamaan Topic dan Event_ID

Penamaan dalam sistem terdistribusi harus unik, collision-resistant, dan meaningful (Tanenbaum & Van Steen, 2017, hlm. 115-120).

**Topic Naming Convention**:
Menggunakan hierarchical naming dengan format `<entity>.<action>`:

```python
TOPICS = [
    "user.login",
    "user.logout", 
    "user.register",
    "order.created",
    "order.completed",
    "order.cancelled",
    "payment.success",
    "payment.failed",
    "inventory.updated",
    "notification.sent"
]
```

Keuntungan:
- Jelas asal dan tujuan event
- Memudahkan filtering (GET /events?topic=user.login)
- Mudah untuk grouping dan analytics

**Event_ID Generation**:
Menggunakan kombinasi timestamp + random suffix:

```python
def generate_event_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"evt_{timestamp}_{random_suffix}"
    # Contoh: evt_20241219082130_a3k9x2m1
```

**Deduplication Key**: Kombinasi `(topic, event_id)` sebagai composite unique key karena event_id yang sama di topic berbeda adalah event yang berbeda.

---

### T5 (Bab 5): Ordering Praktis (Timestamp + Monotonic Counter)

Dalam sistem terdistribusi, total ordering sulit dicapai karena tidak ada global clock (Tanenbaum & Van Steen, 2017, hlm. 145-155).

**Implementasi ordering dalam sistem ini:**

1. **Timestamp ISO8601**: Setiap event memiliki `timestamp` dari source system
   ```json
   "timestamp": "2024-12-19T08:30:00.123456Z"
   ```

2. **Database Sequence (id)**: PostgreSQL BIGSERIAL memberikan monotonic ordering untuk insert order
   ```sql
   id BIGSERIAL PRIMARY KEY  -- Monotonic counter
   ```

3. **received_at Timestamp**: Waktu aktual pemrosesan di aggregator

**Batasan dan Dampak:**

- **Clock Skew**: Timestamp dari publisher mungkin berbeda sedikit dari waktu actual receive. Ditangani dengan menyimpan keduanya (event timestamp + received_at).
  
- **Causal Ordering tidak dijamin**: Jika 3 workers memproses event secara paralel, urutan insert ke database mungkin berbeda dari urutan masuk ke queue. Untuk log aggregation, ini acceptable karena fokusnya adalah completeness.

---

### T6 (Bab 6): Failure Modes dan Mitigasi

Sistem terdistribusi menghadapi berbagai mode kegagalan (Tanenbaum & Van Steen, 2017, hlm. 170-185):

**1. Publisher Failure**
- **Mode**: Publisher crash sebelum selesai mengirim semua events
- **Mitigasi**: Publisher dapat di-restart ulang. Idempotent consumer handles duplicates
- **Implementasi**: Container restart policy `unless-stopped`

**2. Broker (Redis) Failure**
- **Mode**: Redis crash, messages di queue hilang
- **Mitigasi**: Redis AOF persistence dengan appendonly
- **Implementasi**:
  ```yaml
  broker:
    command: redis-server --appendonly yes
    volumes:
      - broker_data:/data
  ```

**3. Consumer Worker Failure**
- **Mode**: Worker crash mid-processing
- **Mitigasi**: Transaction rollback otomatis, event bisa diproses ulang
- **Implementasi**: PostgreSQL ACID transactions

**4. Database Failure**
- **Mode**: PostgreSQL unavailable
- **Mitigasi**: Health checks, connection retry
- **Implementasi**:
  ```yaml
  storage:
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U agguser -d logaggregator"]
      interval: 5s
  ```

**5. Network Partition**
- **Mode**: Services tidak bisa berkomunikasi
- **Mitigasi**: Docker internal network, retry mechanisms
- **Implementasi**: Semua services dalam `uas_network`

---

### T7 (Bab 7): Eventual Consistency; Peran Idempotency + Dedup

Eventual consistency menyatakan bahwa jika tidak ada update baru, semua replika akan eventually mencapai state yang sama (Tanenbaum & Van Steen, 2017, hlm. 200-210).

**Konsistensi dalam Log Aggregator:**

1. **Write Consistency**: Semua writes ke PostgreSQL adalah strongly consistent (single primary node, no replication dalam scope ini).

2. **Read Consistency**: GET /events mengembalikan data yang sudah committed. GET /stats mungkin melihat nilai yang sedang di-update (READ COMMITTED).

3. **Stats Consistency**: Atomic counter updates mencegah lost updates:
   ```sql
   UPDATE stats SET 
       unique_processed_count = unique_processed_count + 1
   WHERE id = 1
   ```

**Hasil Pengujian Konsistensi:**
Dari 20,001 events yang diterima:
- unique_processed (13,001) + duplicate_dropped (7,000) = received_total (20,001) ✓
- Statistik konsisten meski diproses oleh 3 workers secara concurrent

---

### T8 (Bab 8): Desain Transaksi: ACID, Isolation Level

**ACID Compliance dalam Implementasi** (Tanenbaum & Van Steen, 2017, hlm. 230-245):

1. **Atomicity**: Menggunakan PostgreSQL transaction
   ```python
   async with conn.transaction():
       # Insert to processed_events
       # Insert to events
       # Update stats
       # All or nothing
   ```

2. **Consistency**: Unique constraint `(topic, event_id)` menjaga data integrity:
   ```sql
   CONSTRAINT unique_event UNIQUE (topic, event_id)
   ```

3. **Isolation**: READ COMMITTED level (PostgreSQL default)
   - Mencegah dirty reads
   - Cukup untuk use case dedup

4. **Durability**: PostgreSQL WAL + Docker named volumes

**Mengapa READ COMMITTED?**

Trade-off yang dipilih:
- Overhead rendah dibanding SERIALIZABLE
- Unique constraints sudah mencegah duplicate inserts
- Atomic counter updates (`count = count + 1`) aman

---

### T9 (Bab 9): Kontrol Konkurensi: Unique Constraints + Upsert Pattern

**Mekanisme Kontrol Konkurensi** (Tanenbaum & Van Steen, 2017, hlm. 260-275):

**1. Unique Constraints (Primary Mechanism)**
```sql
CONSTRAINT unique_event UNIQUE (topic, event_id)
```
- Database-level enforcement
- Concurrent inserts dengan same key: satu sukses, lainnya conflict
- No race window

**2. ON CONFLICT Pattern (Idempotent Write)**
```sql
INSERT INTO processed_events (topic, event_id, processed_at)
VALUES ($1, $2, NOW())
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id
```
- Atomic check + insert dalam satu statement
- RETURNING id untuk membedakan new insert vs duplicate

**3. Atomic Counter Updates**
```sql
UPDATE stats 
SET received_count = received_count + 1,
    unique_processed_count = unique_processed_count + CASE WHEN $1 THEN 1 ELSE 0 END,
    duplicate_dropped_count = duplicate_dropped_count + CASE WHEN $1 THEN 0 ELSE 1 END
WHERE id = 1
```
Pattern ini mencegah lost-update tanpa pessimistic locking.

**Hasil Test Konkurensi:**
- 3 workers memproses events secara parallel
- 20 unit tests passed termasuk concurrent processing tests
- Tidak ada race condition terdeteksi

---

### T10 (Bab 10-13): Orkestrasi Compose, Keamanan, Persistensi, Observability

**Orkestrasi dengan Docker Compose:**

```yaml
services:
  aggregator:
    depends_on:
      storage:
        condition: service_healthy
      broker:
        condition: service_healthy
```

- **Dependency Management**: Aggregator menunggu PostgreSQL dan Redis healthy
- **Service Discovery**: Internal DNS (storage, broker sebagai hostname)
- **Port Exposure**: Hanya port 8080 (aggregator) untuk akses eksternal

**Keamanan Jaringan:**

```yaml
networks:
  uas_network:
    driver: bridge
```

- Network isolation antar Compose projects
- PostgreSQL (5432) dan Redis (6379) hanya exposed untuk testing
- Production: hapus port mapping untuk database

**Persistensi dengan Named Volumes:**

```yaml
volumes:
  pg_data:
  broker_data:
```

- Data survive container removal
- Hanya dihapus dengan `docker compose down -v`

**Observability:**

1. **Health Endpoint**: `GET /health` untuk liveness probe
2. **Stats Endpoint**: `GET /stats` untuk runtime metrics
3. **Structured Logging**: JSON-formatted logs dengan `structlog`

---

## Arsitektur dan Implementasi

### Diagram Arsitektur

![Arsitektur Sistem](Arch.png)

```
┌─────────────────────────────────────────────────────────────────┐
│                     Docker Compose Network                       │
│                                                                  │
│  ┌────────────┐         ┌────────────┐         ┌────────────┐   │
│  │  Publisher │         │   Broker   │         │ Aggregator │   │
│  │  (Python)  │───RPUSH─│   (Redis)  │◀─BLPOP──│ (FastAPI)  │   │
│  └────────────┘         └────────────┘         └─────┬──────┘   │
│                            Queue                     │          │
│                                                      │ ACID     │
│                                                ┌─────▼──────┐   │
│                                                │  Storage   │   │
│                                                │(PostgreSQL)│   │
│                                                └────────────┘   │
│                                                 Named Volumes   │
└─────────────────────────────────────────────────────────────────┘
```

### Database Schema

```sql
-- Tabel deduplication dengan unique constraint
CREATE TABLE processed_events (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_event UNIQUE (topic, event_id)
);

-- Tabel event data
CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    timestamp TEXT NOT NULL,
    source VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_processed FOREIGN KEY (topic, event_id) 
        REFERENCES processed_events(topic, event_id)
);

-- Statistik dengan single-row constraint
CREATE TABLE stats (
    id INTEGER PRIMARY KEY DEFAULT 1,
    received_count BIGINT DEFAULT 0,
    unique_processed_count BIGINT DEFAULT 0,
    duplicate_dropped_count BIGINT DEFAULT 0,
    started_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT single_row CHECK (id = 1)
);
```

### API Endpoints

| Endpoint | Method | Fungsi |
|----------|--------|--------|
| `/health` | GET | Health check untuk liveness probe |
| `/publish` | POST | Menerima single event |
| `/publish/batch` | POST | Menerima batch events |
| `/events` | GET | Query events dengan filter topic/limit |
| `/stats` | GET | Statistik sistem |

### Alur Pemrosesan Event

1. **Publisher** generate 20,000 events (13,000 unique + 7,000 duplicates)
2. Events di-push ke **Redis queue** dengan RPUSH
3. **3 Consumer workers** compete untuk BLPOP dari queue
4. Worker attempt **INSERT ... ON CONFLICT DO NOTHING**
5. Jika insert berhasil (new) → increment `unique_processed_count`
6. Jika conflict (duplicate) → increment `duplicate_dropped_count`
7. Semua dalam satu **transaction** untuk atomicity

---

## Analisis Performa

### Hasil Stress Test (20,000 Events)

| Metrik | Nilai | Keterangan |
|--------|-------|------------|
| Total Events Sent | 20,000 | Target tercapai ✓ |
| Duplicate Rate | 35% | 7,000 duplicates ✓ |
| Events Received | 20,001 | +1 karena startup event |
| Unique Processed | 13,001 | Semua unique tersimpan ✓ |
| Duplicates Dropped | 7,000 | Semua duplicate terdeteksi ✓ |
| Active Topics | 10 | 10 kategori topic |
| Send Rate | ~95 events/sec | Dikonfigurasi 100 e/s |
| Uptime | 8,761 seconds | Stabil tanpa crash |

### Konsistensi Statistik

```
received_total = unique_processed + duplicate_dropped
20,001 = 13,001 + 7,000 ✓
```

Statistik tetap konsisten meski diproses oleh 3 workers secara concurrent.

### Hasil Unit Test (20 Tests)

| Test Category | Tests | Status |
|---------------|-------|--------|
| API Tests | 5 | ✓ Passed |
| Concurrency Tests | 5 | ✓ Passed |
| Idempotency Tests | 5 | ✓ Passed |
| Persistence Tests | 5 | ✓ Passed |
| **Total** | **20** | **All Passed** |

Test highlights:
- `test_concurrent_processing_same_event`: 10 workers memproses event yang sama, hanya 1 berhasil insert
- `test_no_deadlocks_with_many_concurrent_transactions`: 100 events concurrent tanpa deadlock
- `test_dedup_store_persists_after_connection_reset`: Dedup store survive restart

---

## Kesimpulan

Sistem Pub-Sub Log Aggregator berhasil mengimplementasikan:

1. **Idempotent Consumer** dengan unique constraint `(topic, event_id)` dan `ON CONFLICT DO NOTHING`
2. **Strong Deduplication** - 7,000 duplicates terdeteksi dari 20,000 events (100% akurat)
3. **Transaksi ACID** dengan isolation level READ COMMITTED
4. **Kontrol Konkurensi** - 3 workers tanpa race conditions, 20 tests passed
5. **Persistensi** dengan Docker named volumes
6. **Observability** dengan health checks, stats endpoint, dan structured logging

Sistem telah diuji dengan 20,000 events dan terbukti:
- ✅ Tidak ada data duplikat tersimpan (0 false negatives)
- ✅ Tidak ada unique event yang di-reject (0 false positives)  
- ✅ Statistik konsisten meski concurrent access
- ✅ Data survive container restart

---

## Referensi

Tanenbaum, A. S., & Van Steen, M. (2017). *Distributed Systems: Principles and Paradigms* (3rd ed.). Pearson.

---

## Video Demo

**Link**: https://youtu.be/sp_Zhv_9JWw

**Durasi**: < 25 menit

**Konten demo**:
1. Arsitektur multi-service dan alasan desain
2. Build image dan docker compose up
3. Pengiriman event duplikat dan bukti idempotency
4. Demonstrasi konkurensi multi-worker
5. GET /events dan GET /stats
6. Crash/recreate container + bukti persistensi
7. Observability (logging, metrik)
