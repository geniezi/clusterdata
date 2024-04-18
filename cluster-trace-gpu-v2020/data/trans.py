import pandas as pd

# # 读取CSV文件
# df = pd.read_csv('pai_sensor_table.csv')

# # 根据条件筛选数据行
# filtered_df = df[(df['machine'] == '6f60902edf28bdea3fb8b164')
#                   & (df['gpu_name'] == '/dev/nvidia1')
#                     & (df['gpu_wrk_util']!= 0.0)]

# # 输出筛选后的数据到新的CSV文件
# filtered_df.to_csv('filtered_data.csv', index=False)


# # 读取CSV文件
# df = pd.read_csv('pai_instance_table.csv')

# # 根据条件筛选数据行
# filtered_df = df[(df['machine'] == '6f60902edf28bdea3fb8b164')].sort_values(by='start_time')
#                 #   & (df['gpu_name'] == '/dev/nvidia1')]

# # 输出筛选后的数据到新的CSV文件
# filtered_df.to_csv('filtered_instance.csv', index=False)


# # 读取CSV文件
# df = pd.read_csv('pai_machine_metric.csv')

# # 根据条件筛选数据行
# filtered_df = df[(df['machine'] == '6f60902edf28bdea3fb8b164')]
#                 #   & (df['gpu_name'] == '/dev/nvidia1')]

# # 输出筛选后的数据到新的CSV文件
# filtered_df.to_csv('filtered_machine_metric.csv', index=False)

# 读取两个CSV文件
df1 = pd.read_csv('filtered_data.csv')  # 包含要合并的主要属性
df2 = pd.read_csv('filtered_instance.csv')  # 包含需要添加到df1中的属性
# 假定df1中有 'common_attr' 作为连接键，
# 而df2中有 'attribute1' 和 'attribute2' 要添加到df1中。



merged_df = pd.merge(df1, df2[['worker_name', 'start_time', 'end_time']],
                     on='worker_name',  # 指定连接键
                     how='left').sort_values(by='start_time')  # 左连接，确保df1中的所有记录都被保留

# merged_df=merged_df[merged_df['start_time'] < merged_df['end_time'].shift(-1)]

merged_df['shifted_b'] = merged_df['end_time'].shift(1)
merged_df['shifted_name']= merged_df['task_name'].shift(1)
merged_df['shifted_gpu_wrk_util']= merged_df['gpu_wrk_util'].shift(1)
merged_df=merged_df[(merged_df['start_time'] < merged_df['shifted_b'])
                    &(merged_df['task_name']!= merged_df['shifted_name'])]
# indexes = merged_df.index[merged_df['start_time'] < merged_df['shifted_b']].tolist()
# filtered_indexes = sorted(set(indexes + [i - 1 for i in indexes if i > 0]))
# merged_df = merged_df.loc[filtered_indexes].sort_values(by='start_time')

# 将合并后的数据写入新的CSV文件
merged_df.to_csv('merged_data.csv', index=False)