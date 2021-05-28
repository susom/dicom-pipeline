select distinct accession_number from dcm_qa where customer='Total Joint' and qa_pass='Y' and trunc(qa_date)>trunc(sysdate-7)
