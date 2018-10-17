
import pandas as pd
import numpy as np
import re
from TokChinese import TokChinese




class ZhilianDataPreprocess:
    def __init__(self):
        pass
    
# 删除最后一行和无用的列
    def __general(self,df):
        if 'Unnamed: 0' in df.columns:
            df_new = df.drop(['company_webpage','job_url','scrape_time','Unnamed: 0','feedback_rate','company_address'],inplace= False,axis = 1)
    #         删除最后一行
            df_new = df_new.drop(df.index[-1])
        else:
            df_new = df.drop(['company_webpage','job_url','scrape_time','feedback_rate','company_address'],inplace= False,axis = 1)
    #         删除最后一行
            df_new = df_new.drop(df.index[-1])
        return df_new

    # salary 处理
    # 去掉工资后边的字符，例如'10000-15000元/月\xa0'，保留10000-15000
    def __salary_yuchuli(self,x):
        if (x is not np.nan) and (x is not None): 
            pattern1 = r"(.+)元"
            match1 = re.findall(pattern1,x)
            if len(match1) != 0:
                return match1[0]
            else:
                return str(-1)
        else:
            return str(-1)
        
    # 得到最小工资 ，并转换为数字，该工资为年薪（月薪*15），若为空值，则标为-1   
    def __min_salary(self,x):
        pattern2 = r"(.+)-"
        match2 = re.findall(pattern2,str(x))
        if len(match2) != 0:
            return int(match2[0])*15
        elif x != str(-1):
            return int(x)*15
        else:
            return -1
        
    # 得到最高薪资，并转换为数字，该工资为年薪（月薪*15），若为空值，则标为-1  
    def __max_salary(self,x):
        pattern3 = r".-(.+)"
        match3 = re.findall(pattern3,str(x))
        if len(match3) != 0:
            return int(match3[0])*15
        elif x != str(-1):
            return int(x)*15
        else:
            return -1
        
    #  salary完整处理，输入：df，返回处理好的salary相关列
    def salary_process(self,df):
        df = self.__general(df)
        df['salary'] = df['salary'].apply(self.__salary_yuchuli).apply(lambda x :x.strip())
        df['salary_min'] = df['salary'].apply(self.__min_salary)
        df['salary_max'] = df['salary'].apply(self.__max_salary)
        df['salary_avg'] = (df['salary_min'] + df['salary_max']) / 2
        df.drop('salary',inplace = True,axis = 1)
        return df[['salary_min','salary_max','salary_avg']]
    
#     work_experience预处理，若为空值，则返回‘不限’
    def __work_experience_yuchuli(self,x):
        if (x is not np.nan) and (x is not None): 
            pattern = r"(.+)年"
            match = re.findall(pattern,x)
            if len(match) != 0:
                return match[0]
            else:
                return '不限'
        else:
            return '不限'
#         work_experience完整处理，输入df,返回job_experience的相关列
    def work_experience_process(self,df):
        df = self.__general(df)
        df['job_experience'] = df['work_experience'].apply(self.__work_experience_yuchuli)
        df.drop('work_experience',inplace = True,axis = 1)
        return df[['job_experience']]
    
#     招聘人数处理，去掉最后边的“人”字，如果是空值，则返回-1
    def __demand_yuchuli(self,x):
        if (x is not np.nan) and (x is not None): 
            pattern = r"(.+)人"
            match = re.findall(pattern,x)
            if len(match) != 0:
                return int(match[0])
            else:
                return int(x)
        else:
            return -1
#         demand_number处理，输入df，返回处理好的demand_number
    def demand_number_process(self,df):
        df = self.__general(df)
        df['demand_number'] = df['demand_number'].apply(self.__demand_yuchuli)
        return df[['demand_number']]
    
#     company_scale预处理
    def __company_scale_yuchuli(self,x):
        if (x is not np.nan) and (x is not None): 
            pattern = r"(.+)人"
            match = re.findall(pattern,x)
            if len(match)!= 0:
                return match[0]
            else:
                return -1
        else:
            return -1
        
    def company_scale_process(self,df):
        df = self.__general(df)
        df['company_scale'] = df['company_scale'].apply(self.__company_scale_yuchuli)
        return df[['company_scale']]

#     先处理时间，再处理日期，输入df，返回处理后的发布日期和发布时间
    def publish_date_process(self,df):
        df = self.__general(df)
        df['publish_time'] = df['publish_date'].apply(lambda x :str(x)[10:])
        df['publish_date'] = df['publish_date'].apply(lambda x :str(x)[:10])
        return df[['publish_date','publish_time']]
    
#     把education_degree中的'其他'改为不限
    def education_process(self,df):
        df = self.__general(df)
        df.loc[df['education_degree'] == '其他','education_degree'] = '不限'
        return df[['education_degree']]
#     def jobDescription_seg(self,df):
#         df_new = self.no_change(df)
#         tc = TokChinese()
#         df_new['jobDescriptionSeg'] = df_new['job_description'].apply(lambda x : tc.getAndDedupTokenizedList(x) if x is not np.nan else np.nan)
#         return df[['jobDescriptionSeg']]
#     def skill_seg(self,df):
#         df_new = self.no_change(df)
#         tc = TokChinese()
#         df['skillseg'] = df['job_description'].apply(lambda x : tc.dedup(tc.getEnglishWordsList(x)) if x is not np.nan else np.nan)
#         return df[['skillseg']]
    def welfare_seg(self,df):
        df_new = self.no_change(df)
        tc = TokChinese()
        df_new['welfareSeg'] = df_new['welfare'].apply(lambda x : tc.getAndDedupTokenizedList(x) if x is not np.nan else np.nan)
        return df_new[['welfareSeg']]



#         把未更改的列修改一下列名
    def no_change(self,df):

        df = self.__general(df)
        df_new = df.rename(columns= {'job_name':'welfare',
                           'job_category':'job_name',
                            'company_industrial':'company_industry'})
        df_new['job_description'] = np.nan
        df_new['work_district'] = np.nan
        df_new['skillseg'] = np.nan
        df_new['jobDescriptionSeg'] = np.nan
        return df_new[['job_name','job_description','job_nature','work_position','work_district','company_name','company_industry','company_nature','welfare','jobDescriptionSeg','skillseg']]
    
    
    def data_process(self,df):
        salary = self.salary_process(df)
        job_experience = self.work_experience_process(df)
        demand_number = self.demand_number_process(df)
        company_scale = self.company_scale_process(df)
        publish = self.publish_date_process(df)
        edu = self.education_process(df)
        other = self.no_change(df)
        welfareSeg = self.welfare_seg(df)
        final = pd.concat([other,salary,job_experience,edu,demand_number,company_scale,publish,welfareSeg],axis=1)
        return final


