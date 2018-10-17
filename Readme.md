# Job Analysis Project

This project is about the job analysis in bigdata field between Jan,2018 and Jun,2018 .I mainly use **Pandas **to process the raw data. This is a briefly introduction about the whole data processing process.

## data source 

1. [智联招聘](https://www.zhaopin.com/)

2. [猎聘网](https://www.liepin.com/)


## data info

job_name,job_description,job_nature,job_experience

work_position,work_district	

company_name,company_industry,company_nature,company_scale

welfare,salary_min,salary_max,salary_avg	

education_degree,demand_number,publish_date,publish_time

## file introduction

1. `ZhilianDataPreprocess.py` is the class that is used to process  Zhilian data.
2. `LiepinDataPreprocess.py`is the class that is used to process  Liepin data .
3. `DataAnalysis.py`is the class that is used to analysis the data that has been processed by `ZhilianDataPreprocess.py`and `LiepinDataPreprocess.py`
4. `LiepinDataPreprocess.ipynb`,`ZhilianDataPreprocess.ipynb`and`DataAnalysis.ipynb`is the related introduction about the `py` file.



   