# Data Releases
- Sprint 1: Initial datasets released + students to find their datasets. These will include `consumers`, `transactions` (part 1), and `merchants`. These will be provided in the `tables` directory.
- Sprint 2: Next batch of transactions.
- Sprint 3: Final batch of transactions.
    - A delta list of transactions to be treated as fraud (hence reducing the number of valid transactions and introducing a fraud component) will be added.
    - We may also add a probability of fraud for high-revenue merchants as an optional.

The data will be uploaded directly to the `tables` directory.

## Merchant Data Info
- Revenue Levels: `(a, b, c, d, e)` represents the level of revenue bands (unknown to groups). `a` denotes the smallest band whilst `e` denotes the highest revenue band.
- Take Rate: the fee charged by the BNPL firm to a merchant on a transaction. That is, for each transaction made, a certain percentage is taken by the BNPL firm.
- The dataset has been created to mimic a Salesforce data extract (i.e salespeople will type in tags and segments within a free-text field).
- As such, please be aware of small human errors when parsing the dataset.
- For Example, the tag field may have errors as they were manually input by employees.

## Transaction Data Info
- This is partitioned by the `order_datetime` field. For example, this means you just need to read the `transactions_20210228_20210828` directory, and it will treat the `order_datetime` directories as a partition column (or just another column you can take as is in layman terms).
- There is an underlying distribution with 10% random noise if groups would like to figure it out.
    - This is optional and not expected, though, groups may be able to better predict transaction rates if they find the correct distribution.
- There will be a delta file provided in week 2 representing the records with a fraud outcome.
- These are records that have been flagged and analysed by a Fraud Detection Model and can either be used as an additional feature i.e an `is_fraud` flag or records that can be deleted to keep the transactions data clean.


## Consumer Data Info
- The address field is fake and derived from USA street names. We have included it to mimic a more realistic dataset, but the streets themselves are non-existent and if there are any matches, it will be a pure coincidence.
- The postcode field is accurate and **should** be used for aggregated analysis for joining with other geospatial datasets for demographic information (i.e ABS datasets)
- There is roughly a uniform distribution at the state level (i.e number of consumers per state is the same for all states).

## User to Consumer ID Mapping Table
- Due to a difference between the internal system and a poor design choice (for some reason), the transaction tables use a surrogate key for each new `user_id`.
- However, the Consumer table has a unique ID (some are missing on purpose) field which will require some form of mapping between `consumer_id` to `user_id`.
- An additional mapping table has been provided to join the two datasets together.
