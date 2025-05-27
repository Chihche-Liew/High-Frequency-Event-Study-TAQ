# High-Frequency-Event-Study-TAQ
This SAS script is designed to perform intraday event studies using high-frequency data from the TAQ database. It systematically extracts short-window trading activity and quote information around event timestamps to facilitate detailed market microstructure analysis.

## Features
- **Customizable Event Input**: Specify your own event dataset and date filters.
- **Dynamic Date Matching**: Automatically detects and matches available TAQ data files by date.
- **Trade and Quote Aggregation**:
  - Filters valid trades (`tr_corr in ('00', '01', '02')`, `price > 0`, `size > 0`)
  - Computes volume, trade count, and weighted average price (`WVPRICE`)
  - Joins latest bid/ask quotes before and after the event window
- **Flexible Output**:
  - Save results as SAS dataset or download via `PROC DOWNLOAD`
  - Intermediate downloads every 10 iterations for long batch runs

## Usage
### 1. Set macro variables

At the top of the script, configure the following:

```sas
%let event_lib = WORK;
%let event_ds = your_event_data;
%let event_id_col = ticker;
%let event_year_filter_threshold = 2014;
%let save_option = SASDATASET; /* or DOWNLOAD */
```

### 2. Prepare your event dataset
Your input dataset should include:
- A timestamp column named etimestamp (SAS datetime format)
- A stock identifier (e.g., ticker, cusip, or permno)

### 3. Run the script

## Output Structure
The final dataset contains one row per event with the following columns:
| Column                   | Description                                |
| ------------------------ | ------------------------------------------ |
| gvkey, cusip, permno     | Firm identifiers                           |
| ticker                   | Stock symbol                               |
| date                     | Event date                                 |
| starttime2, endtime2     | Event window start and end (15 min window) |
| datetime\_s, datetime\_e | Actual matched timestamps from TAQ         |
| wvprice\_s, wvprice\_e   | Weighted average price at window start/end |
| volume, trade            | Total volume and trade count in the window |
| bid\_s, ask\_s           | Bid and ask prices at window start         |
| bid\_e, ask\_e           | Bid and ask prices at window end           |

## Notes
- TAQ after 2014 may follow different libname; adjust library name accordingly.
- If multiple TAQ entries match a single event time, the script retains the latest available observation before the event timestamp.
