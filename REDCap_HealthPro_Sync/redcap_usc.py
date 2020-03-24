import pandas as pd
from redcap import Project, RedcapError

def chunked_export(project, chunk_size=10000):
    def chunks(l, n):
        """Yield successive n-sized chunks from list l"""
        for i in range(0, len(l), n):
            yield l[i:i+n]
    record_list = project.export_records(fields=[project.def_field])
    records = [r[project.def_field] for r in record_list]
    try:
        response = []
        for record_chunk in chunks(records, chunk_size):
            chunked_response = project.export_records(records=record_chunk)
            response.extend(chunked_response)
    except RedcapError:
        msg = "Chunked export failed for chunk_size={:d}".format(chunk_size)
        raise ValueError(msg)
    else:
        return response

def export_redcap_records(url, api_key):
	project = Project(url, api_key)
	all_records = chunked_export(project)

	df = pd.DataFrame(all_records)
	return df.filter(['study_id', 'pmi_id', 'dob', 'gift_card_crc_sign_date'])
