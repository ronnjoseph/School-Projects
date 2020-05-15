------This query outputs all categories with count number of reviews per category (verified purchase + vine)

--consolidate all data together
allJPdata = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_multilingual_JP_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
allDEdata = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_multilingual_DE_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
allFRdata = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_multilingual_FR_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
allUKdata = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_multilingual_UK_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
mUSdata = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_multilingual_US_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime); 
a1 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Wireless_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a2 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Watches_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a3 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Video_Games_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a4 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Video_DVD_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a5 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Video_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a6 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Toys_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a7 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Tools_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a8 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Sports_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a9 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Software_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a10 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Shoes_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a11 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Pet_Products_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a12 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Personal_Care_Appliances_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a13 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_PC_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a14 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Outdoors_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a15 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Office_Products_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a16 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Musical_Instruments_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a17 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Music_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a18 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Mobile_Electronics_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a19 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Mobile_Apps_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a20 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Major_Appliances_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a21 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Luggage_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a22 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Lawn_and_Garden_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a23 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Kitchen_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a24 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Jewelry_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a25 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Home_Improvement_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a26 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Home_Entertainment_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a27 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Home_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a28 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Health_Personal_Care_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a29 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Grocery_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a30 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Gift_Card_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a31 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Furniture_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a32 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Electronics_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a33 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Video_Games_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a34 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Video_Download_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a35 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Software_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a36 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Music_Purchase_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a37 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Ebook_Purchase_v1_01.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a38 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Ebook_Purchase_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a39 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Camera_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a40 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Books_v1_02.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a41 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Books_v1_01.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a42 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Books_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a43 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Beauty_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a44 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Baby_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a45 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Automotive_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
a46 = LOAD 's3://amazon-reviews-pds/tsv/amazon_reviews_us_Apparel_v1_00.tsv.gz' USING PigStorage('\t') AS (marketplace:chararray, customer_id:chararray, review_id:chararray, product_id:chararray,product_parent:chararray, product_title:chararray, product_category:chararray, star_rating:int, helpful_votes:int, total_votes:int, vine:chararray, verified_purchase:chararray, review_headline:chararray, review_body:chararray, review_date:datetime);
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
--US filtered reviews with verified purchases and vine reviews
allUSdata = UNION mUSdata,a1,a2,a3,a4,a5,a6,a7,a8,a9,a10,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23,a24,a25,a26,a27,a28,a29,a30,a31,a32,a33,a34,a35,a36,a37,a38,a39,a40,a41,a42,a43,a44,a45,a46;
USdata = FOREACH allUSdata GENERATE marketplace, review_id, product_id, product_title, product_category, star_rating, helpful_votes, vine, verified_purchase, review_date;
USadj = DISTINCT (FILTER USdata BY verified_purchase == 'Y');
vine_USadj= DISTINCT (FILTER USdata BY vine == 'Y');
total_USadj= UNION USadj, vine_USadj;

--A.Count number of all US reviews in each category for all Categories (Stored)
USadjgrp= GROUP total_USadj BY product_category;
A= FOREACH USadjgrp GENERATE $0,COUNT_STAR(total_USadj.product_category) as uscatcount;
rmf s3://664433/output1_US_ALL
STORE A INTO 's3://664433/output1_US_ALL' USING PigStorage ('\t');

--B.Count number of US vine reviews in each category for all Categories (Stored)
USvinegrp= GROUP vine_USadj BY product_category;
B= FOREACH USvinegrp GENERATE $0,COUNT_STAR(vine_USadj.product_category) as usvinecount;
rmf s3://664433/output1_US_ALL_US_vine_total
STORE B INTO 's3://664433/output1_US_ALL_US_vine_total' USING PigStorage ('\t');

--S. Top 4 categories of with the most reviews (United States)
topus= ORDER A BY uscatcount DESC;
S= LIMIT topus 4;
STORE S INTO 's3://664433/output1_US_ALL_top4_US_ALL' USING PigStorage ('\t');

--T. Sorting Product ID and count all reviews (US)
prodIDusgrp= GROUP total_USadj BY product_id; 
prodIDuscount= FOREACH prodIDusgrp GENERATE $0,COUNT_STAR(total_USadj.product_id) as USprodIDcount;
IDcountusdesc= ORDER prodIDuscount BY USprodIDcount DESC;
T= LIMIT IDcountusdesc 50;
STORE T INTO 's3://664433/output1_US_ALL_productcountbyID_hightolow' USING PigStorage ('\t');

--T1,2,3,4: 4 Products in each category for the 4 categories (US)
joinus1= JOIN vine_USadj BY product_category, S BY group;
conjoinus= JOIN joinus1 BY product_id, IDcountusdesc BY group;
---------------------------------------------------------------
--T1 and T3. Products Type 1 and 3 of the 4 categories
--T1
fT1us= FILTER conjoinus BY star_rating >=4;
joingrpus1= GROUP fT1us BY product_category;
T1us= FOREACH joingrpus1 {
	top= TOP(1,6,fT1us);
	GENERATE top;
}
oT1us= FOREACH T1us GENERATE BagToString(fT1us.product_category) as prodcat,BagToString(fT1us.product_id) as prod_id,BagToString(fT1us.product_title),BagToString(fT1us.star_rating),BagToString(fT1us.helpful_votes),BagToString(fT1us.marketplace),BagToString(fT1us.review_id) as rev_id;
STORE oT1us INTO 's3://664433/output1_US_ALL_products_type1' using PigStorage('\t');

--T3
fT3us= FILTER conjoinus BY star_rating >=4 AND helpful_votes == 0;
joingrp3us= GROUP fT3us BY product_category;
T3us= FOREACH joingrp3us {
	top= TOP(1,12,fT3us);
	GENERATE top;
}
oT3us= FOREACH T3us GENERATE BagToString(fT3us.product_category)as prodcat,BagToString(fT3us.product_id) as prod_id,BagToString(fT3us.product_title),BagToString(fT3us.star_rating),BagToString(fT3us.helpful_votes),BagToString(fT3us.marketplace),BagToString(fT3us.review_id) as rev_id;
STORE oT3us INTO 's3://664433/output1_US_ALL_products_type3' using PigStorage('\t');

----------------------------------------------------
--T2 and T4. Products Type 2 and 4 of the 4 categories
--T2  
fT2us= FILTER conjoinus BY star_rating <=2;
joingrp2us= GROUP fT2us BY product_category;
T2us= FOREACH joingrp2us {
	top= TOP(1,6,fT2us);
	GENERATE top;
}
oT2us= FOREACH T2us GENERATE BagToString(fT2us.product_category)as prodcat,BagToString(fT2us.product_id)as prod_id,BagToString(fT2us.product_title),BagToString(fT2us.star_rating),BagToString(fT2us.helpful_votes),BagToString(fT2us.marketplace),BagToString(fT2us.review_id) as rev_id;
STORE oT2us INTO 's3://664433/output1_US_ALL_products_type2' using PigStorage('\t');

--T4
fT4us= FILTER conjoinus BY star_rating <=2 AND helpful_votes == 0;
joingrp4us= GROUP fT4us BY product_category;
T4us= FOREACH joingrp4us {
	top= TOP(1,12,fT4us);
	GENERATE top;
}
oT4us= FOREACH T4us GENERATE BagToString(fT4us.product_category)as prodcat,BagToString(fT4us.product_id)as prod_id,BagToString(fT4us.product_title),BagToString(fT4us.star_rating),BagToString(fT4us.helpful_votes),BagToString(fT4us.marketplace),BagToString(fT4us.review_id) as rev_id;
STORE oT4us INTO 's3://664433/output1_US_ALL_products_type4'using PigStorage('\t');

Usall= UNION oT1us,oT2us,oT3us,oT4us;
Usjoin= JOIN Usall BY prod_id, USdata BY product_id;
Usgrp= GROUP Usjoin BY product_id;

--U1a. Earliest Review Date per product_id
Us1a= FOREACH Usgrp GENERATE BagToString(TOBAG(group)) as prod_id,BagToString(Usjoin.product_title), MIN(Usjoin.review_date) as earlydate;
STORE Us1a INTO 's3://664433/output1_US_ALL_EarliestReviews_PerProductID' using PigStorage('\t');

--U1b. Latest Review Date per product_id
Us1b= FOREACH Usgrp GENERATE BagToString(TOBAG(group)) as prod_id, BagToString(Usjoin.product_title),MAX(Usjoin.review_date) as latestdate;
STORE Us1b INTO 's3://664433/output1_US_ALL_LatestReviews_PerProductID' using PigStorage('\t');

--U2. Vine Review Dates per product_id
Us2join= FILTER Usjoin BY rev_id == review_id; 
Us2grp= GROUP Us2join BY (rev_id, product_id, product_title);
Us2= FOREACH Us2grp GENERATE BagToString(Us2join.prodcat), BagToString(Us2join.review_id),BagToString(Us2join.product_id) as prod_id, BagToString(Us2join.product_title), BagToString(Us2join.review_date);
STORE Us2 INTO 's3://664433/output1_US_ALL_VineReviews_PerProductID' using PigStorage('\t');

--U3. Number of Reviews between Earliest review date and Latest review date and Vine Review Date/Avg Star Rating
--U3a. Number of Reviews between Earliest review date and  Vine Review Date
Us3join= JOIN Us1a BY prod_id, Us1b BY prod_id, Us2 BY prod_id, USdata BY product_id;
Us3ab= FILTER Us3join BY (earlydate <= review_date) AND (review_date <= Us2join.review_date);
Us3abgrp= GROUP Us3ab BY product_id;
Us3a= FOREACH Us3abgrp GENERATE $0,COUNT_STAR($1), AVG(Us3ab.star_rating);
STORE Us3a INTO 's3://664433/output1_US_ALL_reviewcount_and_avgstarrating_btwn_earliest_and_vine_date' using PigStorage('\t');

--U3c. Number of Reviews between Vine Review Date and Latest review date
Us3cd= FILTER Us3join BY (Us2join.review_date <= review_date) AND (review_date <= latestdate);
Us3cdgrp= GROUP Us3cd BY product_id;
Us3c= FOREACH Us3cdgrp GENERATE $0,COUNT_STAR($1), AVG(Us3cd.star_rating);
STORE Us3c INTO 's3://664433/output1_US_ALL_reviewcount_and_avgstarrating_btwn_vine_date_and_latest' using PigStorage('\t');

--U3. Average Star Rating between Earliest review date and Latest review date
Us3fin= FILTER Us3join BY (earlydate <= review_date) AND (review_date <= latestdate);
Us3grp= GROUP Us3fin BY (product_id, product_title);
Us3= FOREACH Us3grp GENERATE BagToString(TOBAG($0)),COUNT_STAR($1), AVG(Us3fin.star_rating);
STORE Us3 INTO 's3://664433/output1_US_ALL_reviewcount_and_avgstarrating_btwn_earliest_and_latest' using PigStorage('\t');

--U4 Total count of reviews per product
Us4= FOREACH Usgrp GENERATE BagToString(TOBAG($0)),COUNT_STAR($1);
STORE Us4 INTO 's3://664433/output1_US_ALL_reviewcount_perproduct' using PigStorage('\t');
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

--UK filtered reviews with verified purchases and vine reviews
UKdata = FOREACH allUKdata GENERATE marketplace, review_id, product_id, product_title, product_category, star_rating, helpful_votes, vine, verified_purchase, review_date;
UKadj = DISTINCT (FILTER UKdata BY verified_purchase == 'Y');
vine_UKadj= DISTINCT (FILTER UKdata BY vine == 'Y');
total_UKadj= UNION UKadj, vine_UKadj;

--C.Count number of all UK reviews in each category for all Categories (Stored)
UKadjgrp= GROUP UKadj BY product_category;
C= FOREACH UKadjgrp GENERATE $0,COUNT_STAR(UKadj.product_category) as ukcatcount;
STORE C INTO 's3://664433/output1_UK_ALL' USING PigStorage ('\t');

--D.Count number of UK vine reviews in each category for all Categories (Stored)
UKvinegrp= GROUP vine_UKadj BY product_category;
D= FOREACH UKvinegrp GENERATE $0,COUNT_STAR(vine_UKadj.product_category) as ukvinecount;
STORE D INTO 's3://664433/output1_UK_ALL_UK_vine_total' USING PigStorage ('\t');

--R. Top 4 categories of with the most reviews (England)
topuk= ORDER C BY ukcatcount DESC;
R= LIMIT topuk 4;
STORE R INTO 's3://664433/output1_UK_ALL_top4_UK_ALL' USING PigStorage ('\t');


--T. Sorting Product ID and count all reviews (UK)
prodIDukgrp= GROUP total_UKadj BY product_id; 
prodIDukcount= FOREACH prodIDukgrp GENERATE $0,COUNT_STAR(total_UKadj.product_id) as UKprodIDcount;
IDcountukdesc= ORDER prodIDukcount BY UKprodIDcount DESC;
T= LIMIT IDcountukdesc 50;
STORE T INTO 's3://664433/output1_UK_ALL_productcountbyID_hightolow' USING PigStorage ('\t');

--T1,2,3,4: 4 Products in each category for the 4 categories (UK)
joinuk1= JOIN vine_UKadj BY product_category, R BY group;
conjoinuk= JOIN joinuk1 BY product_id, IDcountukdesc BY group;
---------------------------------------------------------------
--T1 and T3. Products Type 1 and 3 of the 4 categories
--T1
fT1uk= FILTER conjoinuk BY star_rating >=4;
joingrpuk1= GROUP fT1uk BY product_category;
T1uk= FOREACH joingrpuk1 {
	top= TOP(1,6,fT1uk);
	GENERATE top;
}
oT1uk= FOREACH T1uk GENERATE fT1uk.product_category,fT1uk.product_id,fT1uk.product_title,fT1uk.star_rating,fT1uk.helpful_votes,fT1uk.marketplace;
STORE oT1uk INTO 's3://664433/output1_UK_ALL_products_type1' using PigStorage('\t');

--T3
fT3uk= FILTER conjoinuk BY star_rating >=4 AND helpful_votes == 0;
joingrpuk3= GROUP fT3uk BY product_category;
T3uk= FOREACH joingrpuk3 {
	top= TOP(1,12,fT3uk);
	GENERATE top;
}
oT3uk= FOREACH T3uk GENERATE fT3uk.product_category,fT3uk.product_id,fT3uk.product_title,fT3uk.star_rating,fT3uk.helpful_votes,fT3uk.marketplace;
STORE oT3uk INTO 's3://664433/output1_UK_ALL_products_type3' using PigStorage('\t');

----------------------------------------------------
--T2 and T4. Products Type 2 and 4 of the 4 categories
--T2  
fT2uk= FILTER conjoinuk BY star_rating <=2;
joingrpuk2= GROUP fT2uk BY product_category;
T2uk= FOREACH joingrpuk2 {
	top= TOP(1,6,fT2uk);
	GENERATE top;
}
oT2uk= FOREACH T2uk GENERATE fT2uk.product_category,fT2uk.product_id,fT2uk.product_title,fT2uk.star_rating,fT2uk.helpful_votes,fT2uk.marketplace;
STORE oT2uk INTO 's3://664433/output1_UK_ALL_products_type2' using PigStorage('\t');

--T4
fT4uk= FILTER conjoinuk BY star_rating <=2 AND helpful_votes == 0;
joingrpuk4= GROUP fT4uk BY product_category;
T4uk= FOREACH joingrpuk4 {
	top= TOP(1,12,fT4uk);
	GENERATE top;
}
oT4uk= FOREACH T4uk GENERATE fT4uk.product_category,fT4uk.product_id,fT4uk.product_title,fT4uk.star_rating,fT4uk.helpful_votes,fT4uk.marketplace;
STORE oT4uk INTO 's3://664433/output1_UK_ALL_products_type4' using PigStorage('\t');
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

--FR filtered reviews with verified purchases and vine reviews
FRdata = FOREACH allFRdata GENERATE marketplace, review_id, product_id, product_title, product_category, star_rating, helpful_votes, vine, verified_purchase, review_date;
FRadj = DISTINCT (FILTER FRdata BY verified_purchase == 'Y');
vine_FRadj= DISTINCT (FILTER FRdata BY vine == 'Y');
total_FRadj= UNION FRadj, vine_FRadj;

--G.Count number of all FR reviews in each category for all Categories (Stored)
FRadjgrp= GROUP FRadj BY product_category;
G= FOREACH FRadjgrp GENERATE $0, COUNT_STAR(FRadj.product_category) as frcatcount;
STORE G INTO 's3://664433/output1_FR_ALL' USING PigStorage ('\t');

--H.Count number of FR vine reviews in each category for all Categories (Stored)
FRvinegrp= GROUP vine_FRadj BY product_category;
H= FOREACH FRvinegrp GENERATE $0,COUNT_STAR(vine_FRadj.product_category) as frvinecount;
STORE H INTO 's3://664433/output1_FR_ALL_FR_vine_total' USING PigStorage ('\t');

--P. Top 4 categories of with the most reviews (Germany)
topfr= ORDER G BY frcatcount DESC;
P= LIMIT topfr 4;
STORE P INTO 's3://664433/output1_FR_ALL_top4_FR_ALL' USING PigStorage('\t');

--T. Sorting Product ID and count all reviews (FR)
prodIDfrgrp= GROUP total_FRadj BY product_id; 
prodIDfrcount= FOREACH prodIDfrgrp GENERATE $0,COUNT_STAR(total_FRadj.product_id) as FRprodIDcount;
IDcountfrdesc= ORDER prodIDfrcount BY FRprodIDcount DESC;
T= LIMIT IDcountfrdesc 50;
STORE T INTO 's3://664433/output1_FR_ALL_productcountbyID_hightolow' USING PigStorage ('\t');

--T1,2,3,4: 4 Products in each category for the 4 categories (FR)
joinfr1= JOIN vine_FRadj BY product_category, P BY group;
conjoinfr= JOIN joinfr1 BY product_id, IDcountfrdesc BY group;
---------------------------------------------------------------
--T1 and T3. Products Type 1 and 3 of the 4 categories
--T1
fT1fr= FILTER conjoinfr BY star_rating >=4;
joingrpfr1= GROUP fT1fr BY product_category;
T1fr= FOREACH joingrpfr1 {
	top= TOP(1,6,fT1fr);
	GENERATE top;
}
oT1fr= FOREACH T1fr GENERATE fT1fr.product_category,fT1fr.product_id,fT1fr.product_title,fT1fr.star_rating,fT1fr.helpful_votes,fT1fr.marketplace;
STORE oT1fr INTO 's3://664433/output1_FR_ALL_products_type1' using PigStorage('\t');

--T3
fT3fr= FILTER conjoinfr BY star_rating >=4 AND helpful_votes == 0;
joingrpfr3= GROUP fT3fr BY product_category;
T3fr= FOREACH joingrpfr3 {
	top= TOP(1,12,fT3fr);
	GENERATE top;
}
oT3fr= FOREACH T3fr GENERATE fT3fr.product_category,fT3fr.product_id,fT3fr.product_title,fT3fr.star_rating,fT3fr.helpful_votes,fT3fr.marketplace;
STORE oT3fr INTO 's3://664433/output1_FR_ALL_products_type3' using PigStorage('\t');

----------------------------------------------------
--T2 and T4. Products Type 2 and 4 of the 4 categories
--T2  
fT2fr= FILTER conjoinfr BY star_rating <=2;
joingrpfr2= GROUP fT2fr BY product_category;
T2fr= FOREACH joingrpfr2 {
	top= TOP(1,6,fT2fr);
	GENERATE top;
}
oT2fr= FOREACH T2fr GENERATE fT2fr.product_category,fT2fr.product_id,fT2fr.product_title,fT2fr.star_rating,fT2fr.helpful_votes,fT2fr.marketplace;
STORE oT2fr INTO 's3://664433/output1_FR_ALL_products_type2' using PigStorage('\t');

--T4
fT4fr= FILTER conjoinfr BY star_rating <=2 AND helpful_votes == 0;
joingrpfr4= GROUP fT4fr BY product_category;
T4fr= FOREACH joingrpfr4 {
	top= TOP(1,12,fT4fr);
	GENERATE top;
}
oT4fr= FOREACH T4fr GENERATE fT4fr.product_category,fT4fr.product_id,fT4fr.product_title,fT4fr.star_rating,fT4fr.helpful_votes,fT4fr.marketplace;
STORE oT4fr INTO 's3://664433/output1_FR_ALL_products_type4' using PigStorage('\t');
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------

--DE filtered reviews with verified purchases and vine reviews
DEdata = FOREACH allDEdata GENERATE marketplace, review_id, product_id, product_title, product_category, star_rating, helpful_votes, vine, verified_purchase, review_date;
DEadj = DISTINCT (FILTER DEdata BY verified_purchase == 'Y');
vine_DEadj= DISTINCT (FILTER DEdata BY vine == 'Y');
total_DEadj= UNION DEadj, vine_DEadj;

--G.Count number of all DE reviews in each category for all Categories (Stored)
DEadjgrp= GROUP DEadj BY product_category;
G= FOREACH DEadjgrp GENERATE $0, COUNT_STAR(DEadj.product_category) as decatcount;
STORE G INTO 's3://664433/output1_DE_ALL' USING PigStorage ('\t');

--H.Count number of DE vine reviews in each category for all Categories (Stored)
DEvinegrp= GROUP vine_DEadj BY product_category;
H= FOREACH DEvinegrp GENERATE $0,COUNT_STAR(vine_DEadj.product_category) as devinecount;
STORE H INTO 's3://664433/output1_DE_ALL_DE_vine_total' USING PigStorage ('\t');

--P. Top 4 categories of with the most reviews (Germany)
topde= ORDER G BY decatcount DESC;
P= LIMIT topde 4;
STORE P INTO 's3://664433/output1_DE_ALL_top4_DE_ALL' USING PigStorage('\t');

--T. Sorting Product ID and count all reviews (DE)
prodIDdegrp= GROUP total_DEadj BY product_id; 
prodIDdecount= FOREACH prodIDdegrp GENERATE $0,COUNT_STAR(total_DEadj.product_id) as DEprodIDcount;
IDcountdedesc= ORDER prodIDdecount BY DEprodIDcount DESC;
T= LIMIT IDcountdedesc 50;
STORE T INTO 's3://664433/output1_DE_ALL_productcountbyID_hightolow' USING PigStorage ('\t');

--T1,2,3,4: 4 Products in each category for the 4 categories (DE)
joinde1= JOIN vine_DEadj BY product_category, P BY group;
conjoinde= JOIN joinde1 BY product_id, IDcountdedesc BY group;
---------------------------------------------------------------
--T1 and T3. Products Type 1 and 3 of the 4 categories
--T1
fT1de= FILTER conjoinde BY star_rating >=4;
joingrpde1= GROUP fT1de BY product_category;
T1de= FOREACH joingrpde1 {
	top= TOP(1,6,fT1de);
	GENERATE top;
}
oT1de= FOREACH T1de GENERATE fT1de.product_category,fT1de.product_id,fT1de.product_title,fT1de.star_rating,fT1de.helpful_votes,fT1de.marketplace;
STORE oT1de INTO 's3://664433/output1_DE_ALL_products_type1' using PigStorage('\t');

--T3
fT3de= FILTER conjoinde BY star_rating >=4 AND helpful_votes == 0;
joingrpde3= GROUP fT3de BY product_category;
T3de= FOREACH joingrpde3 {
	top= TOP(1,12,fT3de);
	GENERATE top;
}
oT3de= FOREACH T3de GENERATE fT3de.product_category,fT3de.product_id,fT3de.product_title,fT3de.star_rating,fT3de.helpful_votes,fT3de.marketplace;
STORE oT3de INTO 's3://664433/output1_DE_ALL_products_type3' using PigStorage('\t');

----------------------------------------------------
--T2 and T4. Products Type 2 and 4 of the 4 categories
--T2  
fT2de= FILTER conjoinde BY star_rating <=2;
joingrpde2= GROUP fT2de BY product_category;
T2de= FOREACH joingrpde2 {
	top= TOP(1,6,fT2de);
	GENERATE top;
}
oT2de= FOREACH T2de GENERATE fT2de.product_category,fT2de.product_id,fT2de.product_title,fT2de.star_rating,fT2de.helpful_votes,fT2de.marketplace;
STORE oT2de INTO 's3://664433/output1_DE_ALL_products_type2' using PigStorage('\t');

--T4
fT4de= FILTER conjoinde BY star_rating <=2 AND helpful_votes == 0;
joingrpfr4= GROUP fT4de BY product_category;
T4de= FOREACH joingrpfr4 {
	top= TOP(1,12,fT4de);
	GENERATE top;
}
oT4de= FOREACH T4de GENERATE fT4de.product_category,fT4de.product_id,fT4de.product_title,fT4de.star_rating,fT4de.helpful_votes,fT4de.marketplace;
STORE oT4de INTO 's3://664433/output1_DE_ALL_products_type4' using PigStorage('\t');
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
--JP filtered reviews with verified purchases and vine reviews
JPdata= FOREACH allJPdata GENERATE marketplace, review_id, product_id, product_title, product_category, star_rating, helpful_votes, vine, verified_purchase, review_date;
JPadj = DISTINCT (FILTER JPdata BY verified_purchase == 'Y');
vine_JPadj= DISTINCT (FILTER JPdata BY vine == 'Y');
total_JPadj= UNION JPadj, vine_JPadj;

--I.Count number of all JP reviews in each category for all Categories (Stored)
JPadjgrp= GROUP JPadj BY product_category;
I= FOREACH JPadjgrp GENERATE $0,COUNT_STAR(JPadj.product_category) as jpcatcount;
STORE I INTO 's3://664433/output1_JP_ALL' USING PigStorage ('\t');

--J.Count number of JP vine reviews in each category for all Categories (Stored)
JPvinegrp= GROUP vine_JPadj BY product_category;
J= FOREACH JPvinegrp GENERATE $0,COUNT_STAR(vine_JPadj.product_category) as jpvinecount;
STORE J INTO 's3://664433/output1_JP_ALL_JP_vine_total' USING PigStorage ('\t');

topjp= ORDER I BY jpcatcount DESC;
O= LIMIT topjp 4;
STORE O INTO 's3://664433/output1_JP_ALL_top4_JP_ALL' USING PigStorage('\t');

--T. Sorting Product ID and count all reviews (JP)
prodIDjpgrp= GROUP total_JPadj BY product_id; 
prodIDjpcount= FOREACH prodIDjpgrp GENERATE $0,COUNT_STAR(total_JPadj.product_id) as JPprodIDcount;
IDcountjpdesc= ORDER prodIDjpcount BY JPprodIDcount DESC;
T= LIMIT IDcountjpdesc 50;
STORE T INTO 's3://664433/output1_JP_ALL_productcountbyID_hightolow' USING PigStorage ('\t');

--T1,2,3,4: 4 Products in each category for the 4 categories (JP)
joinjp1= JOIN vine_JPadj BY product_category, O BY group;
conjoinjp= JOIN joinjp1 BY product_id, IDcountjpdesc BY group;
---------------------------------------------------------------
--T1 and T3. Products Type 1 and 3 of the 4 categories
--T1
fT1jp= FILTER conjoinjp BY star_rating >=4;
joingrpjp1= GROUP fT1jp BY product_category;
T1jp= FOREACH joingrpjp1 {
	top= TOP(1,6,fT1jp);
	GENERATE top;
}
oT1jp= FOREACH T1jp GENERATE fT1jp.product_category,fT1jp.product_id,fT1jp.product_title,fT1jp.star_rating,fT1jp.helpful_votes,fT1jp.marketplace;
STORE oT1jp INTO 's3://664433/output1_JP_ALL_products_type1' using PigStorage('\t');

--T3
fT3jp= FILTER conjoinjp BY star_rating >=4 AND helpful_votes == 0;
joingrpjp3= GROUP fT3jp BY product_category;
T3jp= FOREACH joingrpjp3 {
	top= TOP(1,12,fT3jp);
	GENERATE top;
}
oT3jp= FOREACH T3jp GENERATE fT3jp.product_category,fT3jp.product_id,fT3jp.product_title,fT3jp.star_rating,fT3jp.helpful_votes,fT3jp.marketplace;
STORE oT3jp INTO 's3://664433/output1_JP_ALL_products_type3' using PigStorage('\t');

----------------------------------------------------
--T2 and T4. Products Type 2 and 4 of the 4 categories
--T2  
fT2jp= FILTER conjoinjp BY star_rating <=2;
joingrpjp2= GROUP fT2jp BY product_category;
T2jp= FOREACH joingrpjp2 {
	top= TOP(1,6,fT2jp);
	GENERATE top;
}
oT2jp= FOREACH T2jp GENERATE fT2jp.product_category,fT2jp.product_id,fT2jp.product_title,fT2jp.star_rating,fT2jp.helpful_votes,fT2jp.marketplace;
STORE oT2jp INTO 's3://664433/output1_JP_ALL_products_type2' using PigStorage('\t');

--T4
fT4jp= FILTER conjoinjp BY star_rating <=2 AND helpful_votes == 0;
joingrpjp4= GROUP fT4jp BY product_category;
T4jp= FOREACH joingrpjp4 {
	top= TOP(1,12,fT4jp);
	GENERATE top;
}
oT4jp= FOREACH T4jp GENERATE fT4jp.product_category,fT4jp.product_id,fT4jp.product_title,fT4jp.star_rating,fT4jp.helpful_votes,fT4jp.marketplace;
STORE oT4jp INTO 's3://664433/output1_JP_ALL_products_type4' using PigStorage('\t');
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
--consolidated filtered reviews with verified purchases and vine reviews
alldata = UNION total_JPadj,total_DEadj,total_FRadj,total_UKadj,total_USadj;
vine_adj= UNION vine_JPadj,vine_DEadj,vine_FRadj,vine_UKadj,vine_USadj;

--K.Count number of all reviews in each category for all Categories (Stored)
adjgroup= GROUP alldata BY product_category;
K= FOREACH adjgroup GENERATE $0,COUNT_STAR(alldata.product_category) as allcatcount;
STORE K INTO 's3://664433/output1_ALL_all_total' USING PigStorage ('\t');

--L.Count number of vine reviews in each category for all Categories (Stored)
vinegrp= GROUP vine_adj BY product_category;
L= FOREACH vinegrp GENERATE $0,COUNT_STAR(vine_adj.product_category) as allvinecount;
STORE L INTO 's3://664433/output1_ALL_vine_total' USING PigStorage ('\t');

--M.Count all of the different categories (Printed)
catsgroup= GROUP K ALL;
M= FOREACH catsgroup GENERATE CONCAT('Total Number of different categories is ',(chararray)COUNT(K.$0));

--N. Top 4 categories of with the most reviews (consolidated)
toporder= ORDER K BY allcatcount DESC;
N= LIMIT toporder 4;
STORE N INTO 's3://664433/output1_ALL_top4_all' USING PigStorage('\t');

--T. Sorting Product ID and count all reviews
prodIDgrp= GROUP alldata BY product_id; 
prodIDcount= FOREACH prodIDgrp GENERATE $0,COUNT_STAR(alldata.product_id) as allprodIDcount;
IDcountdesc= ORDER prodIDcount BY allprodIDcount DESC;
T= LIMIT IDcountdesc 50;
STORE T INTO 's3://664433/output1_ALL_productcountbyID_hightolow' USING PigStorage ('\t');

--T1,2,3,4: 4 Products in each category for the 4 categories from N (Consolidated)
--------(maybe better to do T1,2,3,4 on per country basis instead of consolidated)------------------------
join1= JOIN vine_adj BY product_category, N BY group;
conjoin= JOIN join1 BY product_id, IDcountdesc BY group;
---------------------------------------------------------------
--T1 and T3. Products Type 1 and 3 of the 4 categories
--T1
fT1= FILTER conjoin BY star_rating >=4;
joingrp1= GROUP fT1 BY product_category;
T1= FOREACH joingrp1 {
	top= TOP(1,6,fT1);
	GENERATE top;
}
oT1= FOREACH T1 GENERATE fT1.product_category,fT1.product_id,fT1.product_title,fT1.star_rating,fT1.helpful_votes,fT1.marketplace;
STORE oT1 INTO 's3://664433/output1_ALL_products_type1' using PigStorage('\t');

--T3
fT3= FILTER conjoin BY star_rating >=4 AND helpful_votes == 0;
joingrp3= GROUP fT3 BY product_category;
T3= FOREACH joingrp3 {
	top= TOP(1,12,fT3);
	GENERATE top;
}
oT3= FOREACH T3 GENERATE fT3.product_category,fT3.product_id,fT3.product_title,fT3.star_rating,fT3.helpful_votes,fT3.marketplace;
STORE oT3 INTO 's3://664433/output1_ALL_products_type3' using PigStorage('\t');

----------------------------------------------------
--T2 and T4. Products Type 2 and 4 of the 4 categories
--T2  
fT2= FILTER conjoin BY star_rating <=2;
joingrp2= GROUP fT2 BY product_category;
T2= FOREACH joingrp2 {
	top= TOP(1,6,fT2);
	GENERATE top;
}
oT2= FOREACH T2 GENERATE fT2.product_category,fT2.product_id,fT2.product_title,fT2.star_rating,fT2.helpful_votes,fT2.marketplace;
STORE oT2 INTO 's3://664433/output1_ALL_products_type2' using PigStorage('\t');

--T4
fT4= FILTER conjoin BY star_rating <=2 AND helpful_votes == 0;
joingrp4= GROUP fT4 BY product_category;
T4= FOREACH joingrp4 {
	top= TOP(1,12,fT4);
	GENERATE top;
}
oT4= FOREACH T4 GENERATE fT4.product_category,fT4.product_id,fT4.product_title,fT4.star_rating,fT4.helpful_votes,fT4.marketplace;
STORE oT4 INTO 's3://664433/output1_ALL_products_type4' using PigStorage('\t');
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
------------------------------------------------------------------------------------
--Filtered reviews with verified purchases and vine reviews (Consolidated without US)
nonUSdata= UNION total_JPadj,total_DEadj,total_FRadj,total_UKadj;
nonUSvine= UNION vine_JPadj,vine_DEadj,vine_FRadj,vine_UKadj;

--K0us.Count number of all reviews in each category for all Categories (Stored)
nonusgroup= GROUP nonUSdata BY product_category;
K0us= FOREACH nonusgroup GENERATE $0,COUNT_STAR(nonUSdata.product_category) as noUScatcount;
STORE K0us INTO 's3://664433/output1_noUS_all_total' USING PigStorage ('\t');

--N. Top 4 categories of with the most reviews (consolidated)
topordernous= ORDER K0us BY noUScatcount DESC;
Nnous= LIMIT topordernous 4;
STORE Nnous INTO 's3://664433/output1_noUS_top4_all' USING PigStorage('\t');

--T. Sorting Product ID and count all reviews
prodIDnousgrp= GROUP nonUSdata BY product_id; 
prodIDnouscount= FOREACH prodIDnousgrp GENERATE $0,COUNT_STAR(nonUSdata.product_id) as noUSprodIDcount;
IDcountnousdesc= ORDER prodIDnouscount BY noUSprodIDcount DESC;
T= LIMIT IDcountnousdesc 50;
STORE T INTO 's3://664433/output1_noUS_productcountbyID_hightolow' USING PigStorage ('\t');

--T1,2,3,4: 4 Products in each category for the 4 categories from N (Consolidated without US)
--------(maybe better to do T1,2,3,4 on per country basis instead of consolidated)------------------------
joinonus= JOIN nonUSvine BY product_category, Nnous BY group;
conjoinonus= JOIN joinonus BY product_id, IDcountnousdesc BY group;
---------------------------------------------------------------
--T1 and T3. Products Type 1 and 3 of the 4 categories
--T1
fT1nous= FILTER conjoinonus BY star_rating >=4;
joingrp1nous= GROUP fT1nous BY product_category;
T1nous= FOREACH joingrp1nous {
	top= TOP(1,6,fT1nous);
	GENERATE top;
}
oT1nous= FOREACH T1nous GENERATE BagToString(fT1nous.product_category) as prodcat,BagToString(fT1nous.product_id) as prod_id,BagToString(fT1nous.product_title),BagToString(fT1nous.star_rating),BagToString(fT1nous.helpful_votes),BagToString(fT1nous.marketplace),BagToString(fT1nous.review_id) as rev_id;
STORE oT1nous INTO 's3://664433/output1_noUS_products_type1' using PigStorage('\t');

--T3
fT3nous= FILTER conjoinonus BY star_rating >=4 AND helpful_votes == 0;
joingrp3nous= GROUP fT3nous BY product_category;
T3nous= FOREACH joingrp3nous {
	top= TOP(1,12,fT3nous);
	GENERATE top;
}
oT3nous= FOREACH T3nous GENERATE BagToString(fT3nous.product_category)as prodcat,BagToString(fT3nous.product_id) as prod_id,BagToString(fT3nous.product_title),BagToString(fT3nous.star_rating),BagToString(fT3nous.helpful_votes),BagToString(fT3nous.marketplace),BagToString(fT3nous.review_id) as rev_id;
STORE oT3nous INTO 's3://664433/output1_noUS_products_type3' using PigStorage('\t');

----------------------------------------------------
--T2 and T4. Products Type 2 and 4 of the 4 categories
--T2  
fT2nous= FILTER conjoinonus BY star_rating <=2;
joingrp2nous= GROUP fT2nous BY product_category;
T2nous= FOREACH joingrp2nous {
	top= TOP(1,6,fT2nous);
	GENERATE top;
}
oT2nous= FOREACH T2nous GENERATE BagToString(fT2nous.product_category)as prodcat,BagToString(fT2nous.product_id)as prod_id,BagToString(fT2nous.product_title),BagToString(fT2nous.star_rating),BagToString(fT2nous.helpful_votes),BagToString(fT2nous.marketplace),BagToString(fT2nous.review_id) as rev_id;
STORE oT2nous INTO 's3://664433/output1_noUS_products_type2' using PigStorage('\t');

--T4
fT4nous= FILTER conjoinonus BY star_rating <=2 AND helpful_votes == 0;
joingrp4nous= GROUP fT4nous BY product_category;
T4nous= FOREACH joingrp4nous {
	top= TOP(1,12,fT4nous);
	GENERATE top;
}
oT4nous= FOREACH T4nous GENERATE BagToString(fT4nous.product_category)as prodcat,BagToString(fT4nous.product_id)as prod_id,BagToString(fT4nous.product_title),BagToString(fT4nous.star_rating),BagToString(fT4nous.helpful_votes),BagToString(fT4nous.marketplace),BagToString(fT4nous.review_id) as rev_id;
STORE oT4nous INTO 's3://664433/output1_noUS_products_type4'using PigStorage('\t');

Uall= UNION oT1nous,oT2nous,oT3nous,oT4nous;
Ujoin= JOIN Uall BY prod_id, nonUSdata BY product_id;
Ugrp= GROUP Ujoin BY product_id;

--U1a. Earliest Review Date per product_id
U1a= FOREACH Ugrp GENERATE BagToString(TOBAG(group)) as prod_id,BagToString(Ujoin.product_title), MIN(Ujoin.review_date) as earlydate;
STORE U1a INTO 's3://664433/output1_noUS_EarliestReviews_PerProductID' using PigStorage('\t');

--U1b. Latest Review Date per product_id
U1b= FOREACH Ugrp GENERATE BagToString(TOBAG(group)) as prod_id, BagToString(Ujoin.product_title),MAX(Ujoin.review_date) as latestdate;
STORE U1b INTO 's3://664433/output1_noUS_LatestReviews_PerProductID' using PigStorage('\t');

--U2. Vine Review Dates per product_id
U2join= FILTER Ujoin BY rev_id == review_id; 
U2grp= GROUP U2join BY (rev_id, product_id, product_title);
U2= FOREACH U2grp GENERATE BagToString(U2join.prodcat), BagToString(U2join.review_id),BagToString(U2join.product_id) as prod_id, BagToString(U2join.product_title), BagToString(U2join.review_date);
STORE U2 INTO 's3://664433/output1_noUS_VineReviews_PerProductID' using PigStorage('\t');

--U3. Number of Reviews between Earliest review date and Latest review date and Vine Review Date/Avg Star Rating
--U3a. Number of Reviews between Earliest review date and  Vine Review Date
U3join= JOIN U1a BY prod_id, U1b BY prod_id, U2 BY prod_id, nonUSdata BY product_id;
U3ab= FILTER U3join BY (earlydate <= review_date) AND (review_date <= U2join.review_date);
U3abgrp= GROUP U3ab BY product_id;
U3a= FOREACH U3abgrp GENERATE $0,COUNT_STAR($1), AVG(U3ab.star_rating);
STORE Us3a INTO 's3://664433/output1_noUS_reviewcount_and_avgstarrating_btwn_earliest_and_vine_date' using PigStorage('\t');

--U3c. Number of Reviews between Vine Review Date and Latest review date
U3cd= FILTER U3join BY (U2join.review_date <= review_date) AND (review_date <= latestdate);
U3cdgrp= GROUP U3cd BY product_id;
U3c= FOREACH U3cdgrp GENERATE $0,COUNT_STAR($1), AVG(U3cd.star_rating);
STORE U3c INTO 's3://664433/output1_noUS_reviewcount_and_avgstarrating_btwn_vine_date_and_latest' using PigStorage('\t');

--U3. Average Star Rating between Earliest review date and Latest review date
U3fin= FILTER U3join BY (earlydate <= review_date) AND (review_date <= latestdate);
U3grp= GROUP U3fin BY (product_id, product_title);
U3= FOREACH U3grp GENERATE BagToString(TOBAG($0)),COUNT_STAR($1), AVG(U3fin.star_rating);
STORE Us3 INTO 's3://664433/output1_noUS_reviewcount_and_avgstarrating_btwn_earliest_and_latest' using PigStorage('\t');

--U4 Total count of reviews per product
U4= FOREACH Ugrp GENERATE BagToString(TOBAG($0)),COUNT_STAR($1);
STORE U4 INTO 's3://664433/output1_noUS_reviewcount_perproduct' using PigStorage('\t');;
------------------------------------------------------------------------------------
DUMP M; --------------Printing total number of categories-----------------------

