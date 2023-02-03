WITH overall AS (SELECT ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY approval_datetime) AS card, * FROM loan_loan WHERE approval_datetime NOTNULL),

prog AS (
SELECT card, LL.user_id, LL.id, FIRST_VALUE(approval_datetime) OVER (PARTITION BY LL.user_id ORDER BY approval_datetime) AS first_loan , approval_datetime,
principal_defaulted/100 AS principal_defaulted, TT.*
FROM overall LL
LEFT JOIN transaction_transaction TT ON LL.id=TT.loan_id AND status = 2 AND transaction_type = 1

)
SELECT DATE_TRUNC('MONTH', first_loan)::DATE AS first_loan_approved, 
SUM(interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100) AS "Total Revenue",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "0",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '1 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "1",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '2 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "2",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '3 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "3",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '4 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "4",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '5 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "5",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '6 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "6",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '7 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "7",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '8 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "8",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '9 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "9",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '10 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "10",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '11 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "11",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '12 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "12",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '13 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "13",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '14 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "14",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '15 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "15",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '16 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "16",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '17 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "17",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '18 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "18",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '19 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "19",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '20 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "20",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '21 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "21",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '22 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "22",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '23 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "23",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '24 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "24",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '25 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "25",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '26 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "26",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '27 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "27",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '28 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "28",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '29 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "29",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '30 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "30",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '31 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "31",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '32 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "32",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '33 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "33",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '34 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "34",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '35 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "35",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '36 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "36",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '37 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "37",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '38 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "38",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '39 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "39",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '40 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "40",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '41 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "41",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '42 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "42",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '43 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "43",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '44 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "44",
SUM(CASE WHEN DATE_TRUNC('MONTH', first_loan) + INTERVAL '45 MONTH' = DATE_TRUNC('MONTH', payment_datetime) THEN interest/100 + write_off_interest/100 + late_fee/100 + write_off_late_fee/100 END) AS "45"
FROM prog
WHERE DATE_TRUNC('MONTH', first_loan) >= {{DATE}}
GROUP BY 1
ORDER BY 1