import pandas as pd
from redcap import Project, RedcapError

def export_healthpro_records(url, api_key):
	project = Project(url, api_key)
	all_records = project.export_records()

	return pd.DataFrame(all_records)