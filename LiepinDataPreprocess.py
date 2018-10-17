
import pandas as pd
import numpy as np
import re
from TokChinese import TokChinese


class LiepinDataPreprocess:
    def __init__(self):
        pass
    
    def __general(self,df):
        df_new = df.drop(['发布网站','原始URL','薪资阶段','公司主页','专业要求','部门'],axis = 1)
        return df_new
    
    def __company_scale_yuchuli(self,x):
        if (x is not np.nan) and (x is not None):
            pattern = r"公司规模：(.+)人"
            match = re.findall(pattern,x)
            if len(match)!= 0:
                return match[0]
        #     有些记录为：公司地址：番禺节能科技园总部2号楼19层 
            else:
                return -1
            
    def company_scale_process(self,df):
        df['company_scale'] = df['公司规模'].apply(self.__company_scale_yuchuli)
        return df[['company_scale']]
    
    def __experience_yuchuli(self,x):
        if (x >= 1) and (x <= 3):
            return '1-3'
        elif x == 0:
            return '不限'
        elif (x > 3) and (x <= 5):
            return '3-5'
        elif (x >5) and (x <= 10):
            return '5-10'
        elif x > 10:
            return '10'
        
    def job_experience_process(self,df):
        df['new_work_experience'] = df['经验'].apply(lambda x :int(x[:-3]) if x[0] != '经' else 0)
        df['job_experience'] = df['new_work_experience'].apply(self.__experience_yuchuli)
        return df[['job_experience']]
    
#     工作地点（市）处理
    def __city_process(self,x):
        if (x is not np.nan) and (x is not None):
            pattern = r"(.+)-"
            if '-' in x:
                match = re.findall(pattern,x)
                return match[0]
            else :
                return x
        else:
            return str(-1)
#   工作地点（区）处理
    def __district_process(self,x):
        if (x is not np.nan) and (x is not None):
            pattern = r"-(.+)"
            match = re.findall(pattern,x)
            if len(match) != 0:
                return match[0]
            else :
                return str(-1)
        else :
            return str(-1)
        
    def workingplace_process(self,df):
        df['work_position'] = df['工作地点'].apply(self.__city_process)
        df['work_district'] = df['工作地点'].apply(self.__district_process)
        return df[['work_position','work_district']]
    
    def education_degree_process(self,df):
        df.loc[df['学历'] == '本科及以上','学历'] = '本科'
        df.loc[df['学历'] == '统招本科','学历'] = '本科'
        df.loc[df['学历'] == '大专及以上','学历'] = '大专'
        df.loc[df['学历'] == '硕士及以上','学历'] = '硕士'
        df.loc[df['学历'] == '学历不限','学历'] = '不限'
        df.loc[df['学历'] == '中专/中技及以上','学历'] = '中专'
        df_new = df.rename(columns = {'学历':'education_degree'})
        return df_new[['education_degree']]
    
    def publish_data_process(self,df):
        df['publish_date'] = df['发布日期'].apply(lambda x : x[:4]+'-'+x[5:7]+'-'+x[8:10]                                              if ((x is not np.nan) or (x is not None)) else str(-1) )  
        df['publish_time'] = np.nan
        return df[['publish_date','publish_time']]
#         最小工资处理
    def __min_salary(self,x):
        if ((x is not np.nan) and (x is not None)):
            pattern2 = r"(.+)-"
            match2 = re.findall(pattern2,str(x))
            if len(match2) != 0:
                return int(match2[0])
            elif '面议' in x:
                return -1
            else:
                return int(x)
        else:
            return -1
    def __max_salary(self,x):
        if ((x is not np.nan) and (x is not None)):
            pattern2 = r".-(.+)"
            match2 = re.findall(pattern2,str(x))
            if len(match2) != 0:
                return int(match2[0])
            elif '面议' in x:
                return -1
            else:
                return int(x)
        else:
            return -1   
#     salary处理 输入 原始数据的df，返回处理后的薪资相关特征
    def salary_process(self,df):
        df['salary_max'] = df['薪资'].apply(self.__max_salary)
        df['salary_min'] = df['薪资'].apply(self.__min_salary)
        df['salary_avg'] = (df['salary_max'] + df['salary_min']) / 2
        return df[['salary_max','salary_min','salary_avg']]
    def other_change(self,df):
        df_new = df.rename(columns = {'职位名称':'job_name',
                                '工作性质':'job_nature',
                                '招聘人数':'demand_number',
                                '职位诱惑':'welfare',
                                '岗位介绍':'job_description',
                                '公司名称':'company_name',
                                '公司行业':'company_industry',
                                 '公司性质':'company_nature'
                                })
        
        return df_new[['job_name','job_nature','demand_number','welfare','job_description','company_name','company_industry','company_nature']]
    def jobDescription_seg(self,df):
        tc = TokChinese()
        df['jobDescriptionSeg'] = df['岗位介绍'].apply(lambda x : tc.getAndDedupTokenizedList(x) if x is not np.nan else np.nan)
        return df[['jobDescriptionSeg']]
    def skill_seg(self,df):
        tc = TokChinese()
        df['skillseg'] = df['岗位介绍'].apply(lambda x : tc.dedup(tc.getEnglishWordsList(x)) if x is not np.nan else np.nan)
        return df[['skillseg']]
    def welfare_seg(self,df):
        tc = TokChinese()
        df['welfareSeg'] = df['职位诱惑'].apply(lambda x : tc.getAndDedupTokenizedList(x) if x is not np.nan else np.nan)
        return df[['welfareSeg']]
    def data_process(self,df):
        df = self.__general(df)
        scale = self.company_scale_process(df)
        experience = self.job_experience_process(df)
        place = self.workingplace_process(df)
        edu = self.education_degree_process(df)
        publish_date = self.publish_data_process(df)
        salary = self.salary_process(df)
        jobDescriptionSeg = self.jobDescription_seg(df)
        skillseg = self.skill_seg(df)
        welfareSeg = self.welfare_seg(df)
        other = self.other_change(df)
        final = pd.concat([other,scale,experience,place,edu,publish_date,salary,jobDescriptionSeg,skillseg,welfareSeg],axis = 1)
        return final
        
        
    
    