# Keck All of Us and HealthPro Sync
Description of files in directory:
`healthpro.py` : Contains a method called export_healthpro_records() which takes project URL (endpoint) and API key as parameters and returns all the records from that project.

Note: The project URL for the above function is a REDCap project that contains all the records from HealthPro that have 'CAL_PMC_USC' as paired_organization.

`redcap_usc.py` : Contains a methods called chunked_export() and export_redcap_records(), which export records in batches and export records wholly.

Note: The purpose of splitting up a large set of records into numerous smaller ones is to reduce the load on the REDCap server and avoid time outs.

`main.py` : This is the main python script that utilizes the above modules. The purpose of this script is to identify those records that are common to both the current instance/snapshot of the records/participants that are in the _Keck All of Us research project_ and _HealthPro_ (HealthPro records in REDCap project).

Note: The reason why the HealthPro records are in a REDCap project is because we do not have access to the HealthPro API yet due to internal IT restrictions.

The main concept behind the logic in _main.py_ is the creation of a LEFT JOIN between the records in HealthPro (LEFT table/dataframe) and records in Keck All of Us project (RIGHT table/dataframe). The JOIN column in this case is a combination of 3 columns: _PMI ID_, _date of birth_ and _appointment date_. Our intention is to relate each HealthPro record with a Keck All of Us study_id. For those HealthPro records which do not have a matching study_id in the Keck AoU project we will programmatically create incremental study_id values and assign it to each of the unmatched records. Finally, we will import all the records into the Keck AoU project.
