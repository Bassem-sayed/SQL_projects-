select * from [DataEcho].[dbo].[heart_cleveland_upload]


-- create new table 
SELECT * into new_table1 from [DataEcho].[dbo].[heart_cleveland_upload]
 SELECT * FROM new_table1

 -- Add Gender type 
  ALTER TABLE new_table1 add Gender varchar(255)
 UPDATE new_table1
 SET Gender= iif(sex=1 ,'Male','Female') FROM new_table1 

  -- Add chest pain type 
  ALTER TABLE new_table1 add cp_type varchar(255)
 UPDATE new_table1
 SET cp_type=  CASE WHEN cp=0 THEN 'Typical Angina' WHEN cp=1 THEN 'A Typical Angina' WHEN cp=2 THEN 'Non-anginal Pain' ELSE 'Asymptomatic' END;

 -- Add exercise induced anina (1=yes , 0= no)
 ALTER TABLE new_table1 add EIA varchar(255)
 UPDATE new_table1
 SET EIA= iif(exang=1 ,'yes' , 'no') FROM new_table1

 -- slope: the slope of the peak exercise ST segmant
 -- value 0: upsloping
 -- value 1: flat
 -- value 2: downsloping

 -- Add new column for slope 
 ALTER TABLE new_table1 add slope1 varchar(255)
 UPDATE new_table1
 SET slope1= CASE WHEN slope=0 THEN 'Upsloping' WHEN slope=1 THEN 'Flat' ELSE 'Downsloping' END;

 -- Add new column for thal(Normal =0 , Fixed Defect=1 , Reversable Defect=2)
  ALTER TABLE new_table1 add NFR varchar(255)
 UPDATE new_table1
 SET NFR= CASE WHEN thal=0 THEN 'Normal' WHEN thal=1 THEN 'Fixed Defect' ELSE 'Reversable Defect' END;

 --  Add new column for condition 0 = NO Disease , 1= Disease 
  ALTER TABLE new_table1 add Disease varchar (255)
 UPDATE new_table1
 SET Disease= iif(condition=1 ,'Disease' , 'NO Disease') FROM new_table1;

  SELECT * FROM new_table1

  -- Create new table 
  CREATE VIEW Hospital AS SELECT age,Gender , cp_type , trestbps , chol,fbs, restecg, thalach, EIA , oldpeak,slope1 , ca, NFR, Disease from new_table1

SELECT * FROM Hospital

SELECT DISTINCT age , COUNT (age) AS COUNT_AGE FROM Hospital GROUP BY age ORDER BY 2;
  
SELECT AVG(age) AS AVG_AGE FROM Hospital; 

SELECT Gender , COUNT(Gender) AS COUNT_Gender FROM Hospital GROUP BY Gender ORDER BY 2;

SELECT Gender , AVG(age) AS AVG_AGE FROM Hospital GROUP BY Gender;

SELECT Gender , cp_type , COUNT(cp_type) AS count_cp_type FROM Hospital GROUP BY Gender , cp_type;

SELECT Gender , trestbps , COUNT(trestbps) AS count_trestbps FROM Hospital GROUP BY Gender , trestbps;

SELECT Gender , EIA , COUNT(EIA) AS count_EIA FROM Hospital GROUP BY Gender , EIA;

SELECT Gender , slope1 , COUNT(slope1) AS count_slope1 FROM Hospital GROUP BY Gender , slope1;

SELECT Gender , ca , COUNT(ca) AS count_ca FROM Hospital GROUP BY Gender , ca;

SELECT Gender , NFR , COUNT(NFR) AS count_NFR FROM Hospital GROUP BY Gender , NFR;

SELECT Gender , Disease , COUNT(Disease) AS count_Disease FROM Hospital GROUP BY Gender , Disease;

SELECT Gender , cp_type , EIA , slope1 , ca, NFR , Disease , COUNT(*) OVER (ORDER BY cp_type) AS NUM  FROM Hospital; 

SELECT Gender , cp_type , EIA , slope1 , ca, NFR , Disease , COUNT(*) OVER (ORDER BY Gender) AS NUM  FROM Hospital;