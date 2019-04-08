USE oneCar_hospital;
DROP TABLE IF EXISTS lab;
CREATE EXTERNAL TABLE lab
(provider_org STRING,
member_id STRING,
date_collected STRING,
test_id STRING,
specialty STRING,
panel STRING,
test_loinc STRING,
test_name STRING,
date_resulted STRING,
specimen STRING,
result_loinc STRING,
result_name STRING,
result_status STRING,
result_description STRING,
numeric_result STRING,
units STRING,
abnormal_value STRING,
reference_range STRING,
order_id STRING,
provider_id STRING,
encounter_id STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/maria_dev/sqooped_tables/lab';

LOAD DATA INPATH 'sqooped_tables/lab'
OVERWRITE INTO TABLE lab;

DROP TABLE IF EXISTS med;
CREATE EXTERNAL TABLE med
(provider_org STRING,
member_id STRING,
last_filled_date STRING,
drug_name STRING,
drug_ndc STRING,
status STRING,
sig STRING,
route STRING,
dose STRING,
units STRING,
order_id STRING,
order_date STRING,
qty_ordered STRING,
refills STRING,
order_provider_id STRING,
order_provider_name STRING,
medication_type STRING,
encounter_id STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/maria_dev/sqooped_tables/med';

LOAD DATA INPATH 'sqooped_tables/med'
OVERWRITE INTO TABLE med;

DROP TABLE IF EXISTS enc;
CREATE EXTERNAL TABLE enc
(provider_org STRING,
encounter_id STRING,
member_id STRING,
provider_id STRING,
provider_npi STRING,
clinic_id STRING,
encounter_dt STRING,
encounter_description STRING,
cc STRING,
episode_id STRING,
patient_dob STRING,
patient_gender STRING,
facility_name STRING,
provider_name STRING,
specialty STRING,
clinic_type STRING,
lab_orders_count STRING,
lab_results_count STRING,
medication_orders_count STRING,
medication_fullfillment_count STRING,
vital_sign_count STRING,
therapy_orders_count STRING,
therapy_actions_count STRING,
immunization_count STRING,
has_appt STRING,
soap_note STRING,
consult_ordered STRING,
disposition STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/maria_dev/sqooped_tables/enc';

LOAD DATA INPATH 'sqooped_tables/enc'
OVERWRITE INTO TABLE enc;

INSERT OVERWRITE DIRECTORY '/user/maria_dev/oozie_task/output'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
SELECT med.member_id, med.drug_name, lab.test_id, enc.provider_id
FROM med
INNER JOIN lab ON med.member_id = lab.member_id
INNER JOIN enc ON med.member_id = enc.member_id
LIMIT 10;
