insert into dcm_pipeline (accession_number, acquired_from, created_on, updated_on, pipeline_id, seqnum, state)
select q.accession_number,'TRY_GEPACS', sysdate, sysdate, 128, dcmpp_seq.nextval,0
from dcm_qa q
where q.customer='Total Joint' and not exists (select 1 from dcm_pipeline p where p.accession_number=q.accession_number)
