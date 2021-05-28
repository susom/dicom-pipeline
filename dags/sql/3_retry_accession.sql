update dcm_pipeline set pipeline_id=128, acquired_from='TRY_GEPACS', state=0 where seqnum in (
    select seqnum from (
    select ACCESSION_NUMBER, max(seqnum) seqnum from dcm_pipeline
    where ACCESSION_NUMBER in (
    select distinct ACCESSION_NUMBER from dcm_qa q where q.customer='Total Joint' and not exists
         (select 1 from dcm_pipeline p where p.accession_number=q.accession_number and p.state in (2,23)))
    group by ACCESSION_NUMBER))
