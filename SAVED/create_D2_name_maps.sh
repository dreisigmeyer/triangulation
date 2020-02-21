#!/bin/bash

# Get the prdn|assg_seq|grant_yr|assg_st|assg_ctry|xml_name|uspto_name|corrected_name|assg_type
awk -F'|' -v OFS='|' '{ if ($4 != "" || $10 != "" || $11 != "") {print $1,$6,$3,$9,$10,$5,$11,$12,$7}}' ../inData/assigneeOutData/*.csv | 
    grep -v "INDIVIDUALLY OWNED PATENT" |
    sort -T ./ -u > prdn_seq_name.csv
sed -i 's/,//g' prdn_seq_name.csv
sed -i 's/|US|/||/g' prdn_seq_name.csv
sed -i 's/|USX|/||/g' prdn_seq_name.csv
sed -i 's/&/AND/g' prdn_seq_name.csv
sed -i 's/+/AND/g' prdn_seq_name.csv
sed -i 's/ \{1,\}/ /g' prdn_seq_name.csv
# Remove non-alphanumeric characters and extra whitespaec
awk -F'|' -v OFS='|' '{ col6=$6;  gsub("[^A-Z0-9 ]","",col6); col7=$7; gsub("[^A-Z0-9 ]","",col7); col8=$8; gsub("[^A-Z0-9 ]","",col8); print $1,$2,$3,$4,$5,col6,col7,col8,$9;}' prdn_seq_name.csv > prdn_seq_stand_name.csv
sed -i 's/|/,/g' prdn_seq_stand_name.csv
rm prdn_seq_name.csv

awk -F'|' -v OFS='|' '{ if ($4 != "" || $10 != "" || $11 != "") {print $1$6,$1,$6,$3,$9,$10,$5,$11,$12,$7}}' ../inData/assigneeOutData/*.csv | 
    grep -v "INDIVIDUALLY OWNED PATENT" |
    sort -T ./ -u > prdn_seq_name.csv
sed -i 's/,//g' prdn_seq_name.csv
sed -i 's/|US|/||/g' prdn_seq_name.csv
sed -i 's/|USX|/||/g' prdn_seq_name.csv
sed -i 's/&/AND/g' prdn_seq_name.csv
sed -i 's/+/AND/g' prdn_seq_name.csv
sed -i 's/ \{1,\}/ /g' prdn_seq_name.csv
# Remove non-alphanumeric characters and extra whitespaec
awk -F'|' -v OFS='|' '{ col7=$7;  gsub("[^A-Z0-9 ]","",col7); col8=$8; gsub("[^A-Z0-9 ]","",col8); col9=$9; gsub("[^A-Z0-9 ]","",col9); print $1,$2,$3,$4,$5,$6,col7,col8,col9,$10;}' prdn_seq_name.csv > ALL_prdnseq_name.csv
sed -i 's/|/,/g' ALL_prdnseq_name.csv
rm prdn_seq_name.csv
mv ALL_prdnseq_name.csv holder.csv
sort -T ./ -t',' -u -k1,1 holder.csv > ALL_prdnseq_name.csv
rm holder.csv
mv D2_prdnseq_name.csv holder.csv
sort -T ./ -t',' -u -k1,1 holder.csv > D2_prdnseq_name.csv
rm holder.csv
join -j 1 -t',' -o 2.2,2.3,2.4,2.5,2.6,2.7,2.8,2.9,2.10,1.2 D2_prdnseq_name.csv ALL_prdnseq_name.csv > D2_name_maps.csv
awk -F',' -v OFS=',' '{if ($6!="") { print $10,$6,$3,"",$4,$5,"D2"}}' D2_name_maps.csv | sort -T ./ -u -t',' >> holder.csv
awk -F',' -v OFS=',' '{if ($7!="") { print $10,$7,$3,"",$4,$5,"D2"}}' D2_name_maps.csv | sort -T ./ -u -t',' >> holder.csv
awk -F',' -v OFS=',' '{if ($8!="") { print $10,$8,$3,"",$4,$5,"D2"}}' D2_name_maps.csv | sort -T ./ -u -t',' >> holder.csv
sort -T ./ -t',' -u holder.csv > sorted_D2_names.csv
rm holder.csv
rm D2_name_maps.csv
rm ALL_prdnseq_name.csv
mv sorted_D2_names.csv D2_name_maps.csv
