# Orders source contract

The demo source is newline-delimited JSON. Each line is one order state as observed in an input batch.

| Field | Type in source | Meaning |
| --- | --- | --- |
| `order_id` | string | Stable business key for the order |
| `customer_id` | string or null | Customer placing the order |
| `order_ts` | timestamp string | Business event time used for daily sales |
| `quantity` | integer string | Number of units; must be greater than zero |
| `unit_price` | decimal string | Price per unit; must be zero or greater |
| `status` | string | Current order status; only `COMPLETE` contributes to sales |
| `updated_at` | timestamp string | Source update time used to select the latest order state |

The generator writes two files. The second batch contains an exact replay, a newer state for an existing order, an older business event that arrives later, and one invalid record. This gives the next pipeline increment concrete scenarios for deduplication, update handling, late data, and quarantine.

The current demo treats `order_id` as the Silver key and selects the record with the greatest `updated_at`. A production source with multiple event types should add a stable `event_id` and explicitly define delete/cancel semantics.

