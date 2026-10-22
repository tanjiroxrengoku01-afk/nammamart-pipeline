-- Reusable SQL functions for the staging layer (same rules as the pandas code in etl_steps.py).

-- Email: first 2 characters of the local part + ***@domain   (priya@example.com -> pr***@example.com)
CREATE OR REPLACE MACRO mask_email(v) AS
    CASE WHEN COALESCE(TRIM(v), '') = '' THEN ''
         WHEN POSITION('@' IN TRIM(v)) = 0 THEN '***'
         ELSE LEFT(split_part(TRIM(v), '@', 1), 2) || '***@' || substr(TRIM(v), POSITION('@' IN TRIM(v)) + 1)
    END;

-- Phone: digits only, keep first 2 and last 2   (9876543221 -> 98******21)
CREATE OR REPLACE MACRO mask_digits(d) AS
    CASE WHEN length(d) = 0 THEN ''
         WHEN length(d) <= 4 THEN repeat('*', length(d))
         ELSE LEFT(d, 2) || repeat('*', length(d) - 4) || RIGHT(d, 2)
    END;
CREATE OR REPLACE MACRO mask_phone(v) AS mask_digits(regexp_replace(COALESCE(v, ''), '[^0-9]', '', 'g'));

-- 'upi', 'UPI ', 'cod', 'CARD' -> allowed values; anything unmappable ('Cashh') -> NULL
CREATE OR REPLACE MACRO clean_payment_mode(v) AS
    CASE LOWER(COALESCE(TRIM(v), ''))
         WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card' WHEN 'cash on delivery' THEN 'Cash on Delivery'
         WHEN 'cod' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet'
    END;

-- 'BRONZE' / 'bronze' -> 'Bronze', 'WEB' -> 'Web'
CREATE OR REPLACE MACRO title_word(v) AS UPPER(LEFT(TRIM(v), 1)) || LOWER(substr(TRIM(v), 2));

CREATE OR REPLACE MACRO txt(v) AS COALESCE(TRIM(v), '');
CREATE OR REPLACE MACRO num(v) AS TRY_CAST(TRIM(v) AS DOUBLE);
