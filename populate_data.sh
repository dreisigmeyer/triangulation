#! /bin/bash
# triangulation input directory
triagIn=""
# input of previous run directory 
priorIn=""
# carra triangulation file directory
carraTring=""
# br match directory
asgnBR=""

cd $triagIn/
rm ./*
rm ./assignee_out_data/*

# copy in data from March 2020 run

# `assg_yr_firmid.csv`: Hand mapping of name to firmid. Can be updated each year automatically (see below on how this is done), or by hand.
cp $priorIn/assg_yr_firmid.csv $triagIn/

# `assignee_76_16.csv`: Created upstream.
#cp $priorIn/assignee_76_17.csv $triagIn/
cp $asgnBR/assignee_76_17.zip $triagIn/
unzip $triagIn/assignee_76_17.zip
rm $triagIn/assignee_76_17.zip

# `assignee_info.csv`: File created from CSV files output from the assignee_prep2 preprocessing phase.
# created below
#cp $priorIn/assignee_info.csv $triagIn/

# `iops.csv`: CSV file created by the assignee_prep2 preprocessing phase.
cp $priorIn/iops.csv $triagIn/

# `name_match.csv`: Created upstream and may need to be preprocessed (see below for how to do this).
#cp $priorIn/name_match.csv $triagIn/
cp $asgnBR/name_match_crosswalk_00_17.zip $triagIn/
unzip $triagIn/name_match_crosswalk_00_17.zip
mv $triagIn/name_match4.csv $triagIn/name_match.csv
rm $triagIn/name_match_crosswalk_00_17.zip

# `prdn_eins.csv`: File created from `name_match.csv` (see below for how to do this).

# `prdn_metadata.csv`: CSV file output from the carra_prep2 preprocessing phase.
cp $priorIn/prdn_metadata.csv $triagIn/

#cp $priorIn/carra_for_triangulation.csv $triagIn/
cp $carraTring/carra_for_triangulation.csv $triagIn/

cp $priorIn/assigneeOutData/* $triagIn/assignee_out_data/

# `prdn_piks.csv`: File created from `carra_for_triangulation.csv` (see below for how to do this). 

# preprocessing steps from readme
awk -F'|' -v OFS=',' '{print $1,$4}' iops.csv | sort -T ./ -u > iops_prdn_assg_seq.csv  
# carra_for_triangulation  
tail -n +2 carra_for_triangulation.csv > holder.csv  
awk -F',' -v OFS=',' '{print $10,$6,$5,$8,$3,$4,$7,$1}' holder.csv > prdn_piks.csv
rm holder.csv  
# Need to create firmid in name_match file and remove last two columns:  
awk -F, -v OFS=, '{ print $1,$2,$3,$13,$4,$5,$11,$7,$8}' name_match.csv > name_match_HOLD.csv  
mv name_match.csv name_match.csv_SAVED  
tail -n +2 name_match_HOLD.csv > name_match.csv  
awk -F',' -v OFS=',' '{print $1,$4,$3,$6,$7,$9,$8}' name_match.csv > prdn_eins.csv  
rm name_match_HOLD.csv  
# Extend assg_yr_firmid.csv to new year  
awk -F',' -v OFS=',' '{ if ($2==2015) {print $1,2016,$3"\n"$1,2017,$3}}' assg_yr_firmid.csv > holder.csv  
cat holder.csv >> assg_yr_firmid.csv  
rm holder.csv
# Create assignee_info.csv with structure PRDN,ASSG_NUM,ASSG_TYPE,ST,CTRY:  
awk -F'|' -v OFS=',' '{print $1,$6,$7,$9,$10}' assignee_out_data/*.csv > assignee_info.csv
# Cut last column off prdn_metadata.csv then do  
sort -T ./ -u prdn_metadata.csv > holder  
mv holder prdn_metadata.csv  
# For F models
awk -F'|' -v OFS='|' '{ if ($4 != "" || $10 != "" || $11 != "") {print $1,$6,$3,$9,$10,$5,$11,$12,$7}}' assignee_out_data/*.csv |  grep -v "INDIVIDUALLY OWNED PATENT" | sort -u -T ./ > prdn_seq_name.csv  
sed -i 's/|US|/||/g' prdn_seq_name.csv  
sed -i 's/|USX|/||/g' prdn_seq_name.csv  
sed -i 's/&/AND/g' prdn_seq_name.csv  
sed -i 's/+/AND/g' prdn_seq_name.csv  
sed -i 's/ \{1,\}/ /g' prdn_seq_name.csv  
# Remove non-alphanumeric characters and extra whitespace  
awk -F'|' -v OFS='|' '{ col6=$6;  gsub("[^A-Z0-9 ]","",col6); col7=$7; gsub("[^A-Z0-9 ]","",col7); col8=$8; gsub("[^A-Z0-9 ]","",col8); print $1,$2,$3,$4,$5,col6,col7,col8,$9;}' prdn_seq_name.csv > prdn_seq_stand_name.csv  
sed -i 's/|/,/g' prdn_seq_stand_name.csv  
rm prdn_seq_name.csv  
# For the final crosswalk construction
awk -F'|' -v OFS=',' '{ if ($10 ~ /US/) { print $1,$6,"",$4,$3,$7,$9,$10,1,0,"","","","","","","" } else if ($9 != "") {print $1,$6,"",$4,$3,$7,$9,$10,1,0,"","","","","","","" } else if ($10 != "") {print $1,$6,"",$4,$3,$7,$9,$10,0,1,"","","","","","","" } else { print $1,$6,"",$4,$3,$7,$9,$10,0,0,"","","","","","","" } }' assignee_out_data/*.csv | sort -T ./ -u > full_frame.csv
sed -i '1s/^/prdn,assg_seq,firmid,app_yr,grant_yr,assg_type,assg_st,assg_ctry,us_assg_flag,foreign_assg_flag,us_inv_flag,num_assg,cw_yr,emp_yr,model,uniq_firmid,num_inv\n/' full_frame.csv
