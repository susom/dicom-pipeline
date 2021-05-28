update dcm_qa
    set delivery_batch=1, qa_pass='Y', qa_date=sysdate, pass_fail_reason='Series count in GCP >= CFIND'
where ACCESSION_NUMBER in (
    select distinct p.accession_number
    from dcm_qa h, dcm_pipeline p
    where h.customer='Total Joint'
      and p.accession_number=h.accession_number
      and p.related_seq is null and state in (2,23)
      and p.STUDY_RELATED_instances-h.gcp_srs<=3)
  and customer='Total Joint'
