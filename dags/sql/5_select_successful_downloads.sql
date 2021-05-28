select p.accession_number from dcm_qa h, dcm_pipeline p
where h.customer in ('Total Joint') and p.accession_number=h.accession_number and state in (2,23)
and trunc(UPDATED_ON) > trunc(SYSDATE -14)

