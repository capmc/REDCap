import pandas as pd
from redcap import Project, RedcapError
from redcap_usc import export_redcap_records
from healthpro import export_healthpro_records
from redcaphelper import Connection
import pprint

import time
start_time = time.time()

URL = 'https://redcap.med.usc.edu/api/'	# base URL API endpoint
REDCAP_API_KEY = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'	# API key for the project with All of Us project records
HP_API_KEY = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxx'	# API key for the REDCap project which contains HealthPro records

if __name__ == "__main__":
	# export all HealthPro records which have 'CAL_PMC_USC' as paired organization
	# in this case, we have those records stored on a separate project in REDCap
	hp_recs = export_healthpro_records(URL, HP_API_KEY)

	# export all records that have been created as part of the patient/participant onboarding
	# process at USC's Keck hospital (AoU records)
	rc_recs = export_redcap_records(URL, REDCAP_API_KEY)

	# create a LEFT JOIN between HealthPro records and exisiting AoU records
	# columns on which we have performed the JOIN are `pmi_id` and `dob`
	hp_rc_merged = pd.merge(hp_recs, rc_recs, left_on=['pmi_id', 'dob', 'physical_measurements_complete_date'], 
											  right_on=['pmi_id', 'dob', 'gift_card_crc_sign_date'], 
											  how='left')
	print(hp_rc_merged)

	# to count the number of records which have not been assigned any 'study_id' (study_id == NaN)
	unassigned_recs = len(hp_rc_merged) - hp_rc_merged['study_id'].count()

	print("No. of records which could not be matched with a `study_id`: " + str(unassigned_recs))

	# study_id's that need to be manually resolved because there are no matching columns while
	# creating JOIN
	exclude = ['162890', '19209', '87121']
	hp_rc_merged = hp_rc_merged[~hp_rc_merged['study_id'].isin(exclude)]

	# identify duplicates which still remain after the matching
	hp_rc_merged_dups = hp_rc_merged[hp_rc_merged.duplicated(subset=['pmi_id', 'dob', 'gift_card_crc_sign_date'], keep=False)]
	print(hp_rc_merged_dups)

	# prepare dataframe from those records which have a matching `study_id`
	# i.e., ones without NaN in their study_id column
	hp_rc_not_nulls = hp_rc_merged[hp_rc_merged['study_id'].notnull()]
	hp_rc_not_nulls.columns = ['hp_' + str(col) if col != "study_id" else str(col) for col in hp_rc_not_nulls.columns]
	print(hp_rc_not_nulls)
	# hp_rc_not_nulls.to_csv('records_matched.csv', index=False)

	# prepare dataframe from those records which have NaN value in their study_id column
	hp_rc_nulls = hp_rc_merged[hp_rc_merged['study_id'].isna()]

	# compute maximum value of study_id that is present among the records in exisiting AoU instance/project
	max_study_id = rc_recs['study_id'].apply(pd.to_numeric).max()
	print("Maximum study id is: " + str(max_study_id))

	# increment the study_id so we can assign values to records with missing study_id's
	max_study_id = max_study_id + 1
	hp_rc_nulls['study_id'] = max_study_id + range(len(hp_rc_nulls))
	hp_rc_nulls.columns = ['hp_' + str(col) if col != "study_id" else str(col) for col in hp_rc_nulls.columns]
	print(hp_rc_nulls)
	# hp_rc_nulls.to_csv('nulls.csv', index=False)

	not_null_dicts = []	# list of dicts that will represent the records in the hp_rc_not_nulls dataframe
	not_null_dicts = hp_rc_not_nulls.to_dict('records')

	# delete key/value pair associated with 'record_id' from list of dicts
	for d in not_null_dicts:
		del d['hp_record_id']

	null_dicts = []	# list of dicts that will represent the records in the hp_rc_nulls dataframe
	null_dicts = hp_rc_nulls.to_dict('records')

	# delete key/value pair associated with 'record_id' from list of dicts
	for d in null_dicts:
		del d['hp_record_id']

	# create object of type Connection
	# use the API_KEY of the REDCap project to which you want to write records to
	conn = Connection(URL, REDCAP_API_KEY)
	# specify the size of the chunks in which you want to import the new records using `chunk_sz`
	conn.import_records_chunked(not_null_dicts, chunk_sz=500)
	conn.import_records_chunked(null_dicts, chunk_sz=500)

	print("--- %s seconds ---" % (time.time() - start_time))