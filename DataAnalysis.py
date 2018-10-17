

from ZhilianDataPreprocess import ZhilianDataPreprocess
from LiepinDataPreprocess import LiepinDataPreprocess
from TokChinese import TokChinese
import pandas as pd
import numpy as np
import re



'''在使用ZhilianDataPreprocess和LiepinDataPreprocess两个
类分别处理完数据之后，将数据汇总然后调用该类，完成数据分析工作。
'''
import pandas as pd
import numpy as np
import re
from wordcloud import WordCloud,ImageColorGenerator # 词云包
import matplotlib as mpl
from matplotlib import pyplot as plt
get_ipython().magic('matplotlib inline')

class DataAnalysis:
    def __init__(self):
        pass
    def functionDescription(self):
        print('introduction about each function:')
        print('salaryIntervalAmountAndProportion:将薪资按照区间分段，输出每个区间段内的人员数量和比例。')
        print('educationAmountAndProportionAndSalary:输出不同学历求值人员的数量、比例及平均薪资')
        print('eduAndJobexperienceAndSalary:不同学历不同工作经验对应的平均薪资')
        print('jobCategoryAmountAndProportion:不同工作类别的人员数量及比例')
        print('jobCategoryAndJobexperienceAndSalary:不同工作类别不同工作经验对应的平均薪资')
        print('jobExperienceAndAmountAndProportion:不同工作经验的人员数量及比例')
        print('workPositionAndAmount:不同地点的求职人员数量')
        print('workPositionAndJobfenleiAndSalary:不同地点不同工作种类的平均薪资')
        print('workPositionAndSalary:不同地点的平均薪资')
        print('companyindustryAndAmount:不同行业的求职人员数量')
        print('jobDescriptionAnalysis:统计职位描述中的词语出现频数')
        print('jobDescriptionWordCloud:绘制词云')
        print('jobCategoryAndSalary:统计每种工作的薪资')
    def salaryIntervalAmountAndProportion(self,df):
        df1 = pd.DataFrame()
        df1['salary_avg_cut'] = pd.cut(df['salary_avg'],bins = [0,50000,100000,150000,200000,300000,500000,2200000])                             
        tm1 = df1['salary_avg_cut'].value_counts().to_frame().reset_index()
        tm1.columns = ['薪资区间','人员数量']
        tm2 = df1['salary_avg_cut'].value_counts(normalize = True).to_frame().reset_index()
        tm2.columns = ['薪资区间','数量占比']
        tm3 = tm1.merge(tm2,how = 'inner',on = '薪资区间')
        return tm3
    def educationAmountAndProportionAndSalary(self,df):
        edu1 = df['education_degree'].value_counts().to_frame().reset_index()
        edu1.columns = ['学历','人员数量']
        edu2 = df['education_degree'].value_counts(normalize = True).to_frame().reset_index()
        edu2.columns = ['学历','数量占比']
        edu3 = df.groupby('education_degree')[['salary_avg']].mean().sort_values('salary_avg',ascending = False).applymap(lambda x:int(x)).reset_index()
        edu3.columns = ['学历','平均薪资']
        edu4 = edu1.merge(edu2,how = 'inner',on = '学历').merge(edu3,how = 'inner',on = '学历')
        return edu4
    def eduAndJobexperienceAndSalary(self,df):
        result = df.groupby(['education_degree','job_experience'])[['salary_avg']].mean().applymap(lambda x:int(x)).unstack()
        return result
    def __jobname_fenlei(self,x):
        if  (x is not np.nan) and (x is not None):
            leibie = ['测试','开发','产品','前端','算法','数据','后端','运维','UI','研发','维护','数据库','运营','软件','机器学习','网络']
            i = 0
            for item in leibie:
                if item in x:
                    return item
                    i = 1
                    break
            if i == 0:
                return np.nan
        else:
            return np.nan
    def jobCategoryAmountAndProportion(self,df):
        df['jobname_fenlei'] = df['job_name'].apply(self.__jobname_fenlei)
        df.loc[df['jobname_fenlei'] == '维护','jobname_fenlei'] = '运维'
        df.loc[df['jobname_fenlei'] == '研发','jobname_fenlei'] = '开发'
        job1 = df['jobname_fenlei'].value_counts().to_frame().reset_index()
        job1.columns = ['职位','人员数量']

        job2 = df['jobname_fenlei'].value_counts(normalize = True).to_frame().reset_index()
        job2.columns = ['职位','人员比例']

        job3 = job1.merge(job2,how = 'inner',on = '职位')
        return job3
    def jobCategoryAndJobexperienceAndSalary(self,df):
        df1 = df[pd.notnull(df['jobname_fenlei'])]
        result = df1.groupby([df1['jobname_fenlei'],df1['job_experience']])[['salary_avg']].mean().applymap(lambda x :int(x)if x is not np.nan else -1).unstack()
        return result
    def jobExperienceAndAmountAndProportion(self,df):
        tm1 = df['job_experience'].value_counts().to_frame().reset_index()
        tm1.columns = ['工作经验','人员数量']

        tm2 = df['job_experience'].value_counts(normalize = True).to_frame().reset_index()
        tm2.columns = ['工作经验','人员比例']

        tmp3 = tm1.merge(tm2,how = 'inner',on = '工作经验')
        return tmp3
    def workPositionAndAmount(self,df):
        tmp4 = df.groupby('work_position')[['job_name']].count().sort_values('job_name',ascending = False).reset_index()
        tmp4.columns = ['工作地点','人员数量']
        return tmp4
    def workPositionAndJobfenleiAndSalary(self,df):
        tm5 = df.groupby(['work_position','jobname_fenlei'])[['salary_avg']].mean().applymap(lambda x :int(x)).unstack()
        return tm5
    def workPositionAndSalary(self,df):
        tmp6 = df.groupby('work_position')[['salary_avg']].mean().sort_values('salary_avg',ascending = False).applymap(lambda x: int(x))
        tmp6.reset_index(inplace = True)
        tmp6.columns = ['工作地点','平均工资']
        return tmp6
    def companyindustryAndAmount(self,df):
        tmp7 = df.groupby('company_industry')[['job_name']].count().sort_values('job_name',ascending = False)
        tmp7.reset_index(inplace = True)
        tmp7.columns = ['公司行业','人员数量']
        return tmp7
    def jobDescriptionAnalysis(self,df):
        tmp8 = df[pd.notnull(df['job_description'])] [['job_description','jobDescriptionSeg']]
        tmp8.index = np.arange(0,len(tmp8))
#         tmp8['new_job_description'] = tmp8['jobDescriptionSeg'].apply(lambda x : x[2:-2].split("', '"))
        all_words = []
        for i in tmp8.index:
            all_words.extend(tmp8.loc[i,'jobDescriptionSeg'])
        words_df = pd.DataFrame({'all_words':all_words})   
        words_stat = words_df.groupby(by=['all_words'])['all_words'].agg({"计数":np.size})
        words_stat=words_stat.reset_index().sort_values(by=["计数"],ascending=False)
        words_stat.drop([words_stat.loc[(words_stat['all_words'] == ' '),:].index[0],words_stat.loc[words_stat['all_words'] == '\r\n'].index[0], words_stat.loc[words_stat['all_words'] == '\n'].index[0]],inplace= True)
        return words_stat
    def jobDescriptionWordCloud(self,df):
        word_stat = self.jobDescriptionAnalysis(df)
        mpl.rc('figure', figsize = (14, 7))
        mpl.rc('font', size = 14)
        mpl.rc('axes', grid = False)
        mpl.rc('axes', facecolor = 'white')
        wordcloud = WordCloud(font_path="simheittf.ttf"
                      ,background_color="white"
                      ,max_font_size=80)
        word_frequence={x[0]:x[1] for x in word_stat.head(1000).values}
        wordcloud=wordcloud.fit_words(word_frequence)
        plt.imshow(wordcloud)
    def jobCategoryAndSalary(self,df):
        df['jobname_fenlei'] = df['job_name'].apply(self.__jobname_fenlei)
        tmp8 = df.groupby('jobname_fenlei')[['salary_avg']].mean().applymap(lambda x :int(x))
        tmp8.reset_index(inplace = True)
        tmp8.columns = ['职位类别','平均薪资']
        return tmp8
        
        


