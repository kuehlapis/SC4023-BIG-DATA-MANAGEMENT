# SC4023 Project Design

## 1) Objective

Build a column-oriented program that scans resale flat records and outputs all valid `(x, y)` pairs and their matched minimum-price-per-sqm record, based on one group member's matriculation number.

---

## 2) Data Storage Contract

### 2.1 Column Files

Directory: `Database/`

Required files (10):

- `month.col`
- `town.col`
- `flat_type.col`
- `block.col`
- `street_name.col`
- `storey_range.col`
- `floor_area_sqm.col`
- `flat_model.col`
- `lease_commence_date.col`
- `resale_price.col`

### 2.2 Row Alignment Rule

All column files must have the same number of rows `N`.
For any row index `i` (0-based), values from all 10 columns at index `i` represent one transaction record.

### 2.3 Data Types

- `month`: string, format like `Jan-15`
- `town`: string
- `flat_type`: string
- `block`: string
- `street_name`: string
- `storey_range`: string
- `floor_area_sqm`: float
- `flat_model`: string
- `lease_commence_date`: int
- `resale_price`: float

### 2.4 Derived Fields (Processing Phase)

- `year` (int), `month_num` (int) parsed from `month`
- `price_per_sqm` = `resale_price / floor_area_sqm` (float)

---

## 3) Query Contract

### 3.1 Inputs

- `matric_num: str`
- `x` in `[1..8]` (months window length)
- `y` in `[80..150]` (minimum floor area)

### 3.2 Conditions

For each `(x, y)`:

1. Time window is determined by matric number:
   - target year from last digit of matric number (mapped to 2015..2024; not 2025 as start year)
   - start month from second-last digit (`0` means October)
   - include months from start to start + x - 1
2. `town` must be in towns mapped from digits in matric number.
3. `floor_area_sqm >= y`.

### 3.3 Selection Rule

Among matched rows, choose row with minimum `price_per_sqm`.

Tie-breaker (deterministic):

1. smaller `price_per_sqm`
2. if tied, smaller row index

### 3.4 Valid Pair Rule

`(x, y)` is valid only if:

- at least one matched row exists, and
- minimum `price_per_sqm <= 4725`

---

## 4) Output Contract

File name:

- `ScanResult_<MatricNum>.csv`

Columns:

- `(x, y),Year,Month,Town,Block,Floor_Area,Flat_Model,Lease_Commence_Date,Price_Per_Square_Meter`

Sort order:

1. increasing `x`
2. for same `x`, increasing `y`

No result handling:

- If there is no qualified entry for a pair, output `No result` for that pair according to the final agreed formatting.

Rounding:

- `Price_Per_Square_Meter` rounded to nearest integer.

---

## 5) Error and Edge Handling

- If any required `.col` file is missing: fail fast with a clear message.
- If column lengths mismatch: fail fast.
- If numeric parse fails: skip malformed row and optionally log row index.
- If `floor_area_sqm <= 0`: skip row to avoid divide-by-zero.
- If no matched row for `(x, y)`: return no result for that pair.

---

## 6) Module Ownership (3 Members)

- Member A: storage + loader + validation
- Member B: condition generation + filter/mask + minimum selection
- Member C: output writer + correctness checks + report evidence

Integration checkpoints:

- CP1: storage contract frozen
- CP2: query engine returns in-memory result rows
- CP3: final CSV + validation evidence