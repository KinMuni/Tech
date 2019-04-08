use onecar_hospital;
drop table if exists enc;
create table enc( 
	provider_org text,
	encounter_id text,
	member_id text,
	provider_id text,
	provider_npi text,
	clinic_id text,
	encounter_datetime text,
	encounter_description text,
	cc text,
	episode_id text,
	patient_dob text,
	patient_gender text,
	facility_name text,
	specialty text,
	clinic_type text,
	lab_orders_count text,
	medication_orders_count text,
	medication_fulfillment_count text,
	vital_sign_count text,
	therapy_orders_count text,
	therapy_actions_count text,
	immunization_count text,
	has_appt text,
	soap_note text,
	consult_ordered text,
	disposition text 
);

load data local infile 'enc.csv' into table enc
	fields terminated by ','
	optionally enclosed by '"'
	lines terminated by '\n'
	ignore 1 lines;

drop table if exists enc_dx;
create table enc_dx (
	provider_org text,
	code text,
	vocab text,
	description text,
	severity text,
	encounter_id text);

load data local infile 'enc_dx.csv' into table enc_dx
	fields terminated by ','
	optionally enclosed by '"'
	lines terminated by '\n'
	ignore 1 lines;

drop table if exists lab;
create table lab (
	provider_org text,
	member_id text,
	date_collected text,
	test_id text,
	specialty text,
	test_loinc text,
	test_name text,
	date_resulted text,
	specimen text,
	result_loinc text,
	result_name text,
	result_state text,
	numeric_result text,
	units text,
	abnormal_value text,
	reference_range text,
	order_id text,
	provider_id text,
	encounter_id text);

load data local infile 'lab.csv' into table lab
	fields terminated by ','
	optionally enclosed by '"'
	lines terminated by '\n'
	ignore 1 lines;

drop table if exists med;
create table med (
	provider_org text,
	member_id text,
	last_filled_date text,
	drug_name text,
	drug_ndc text,
	status text,
	sig text,
	route text,
	dose int,
	units text,
	order_id text,
	order_date text,
	qty_ordered text,
	refills text,
	order_provider_id text,
	order_provider_name text,
	medication_type text,
	encounter_id text);

load data local infile 'med.csv' into table med
	fields terminated by ','
	optionally enclosed by '"'
	lines terminated by '\n'
	ignore 1 lines;
