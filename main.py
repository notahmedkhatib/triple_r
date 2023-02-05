import pandas as pd
import r3

df_reporters = pd.read_csv('Test_Reporters.csv')
df_remove = pd.read_csv('Job_Remove.csv')
df_jobs = pd.read_csv('Job_Curation.csv')

# df_reporters[['new_designation', 'flags']] = r3.replace(df_reporters, 'designation', df_jobs['original'], df_jobs['replacement'], updated_name='new_designation')
# df_reporters[['new_designation', 'flags']] = r3.remove(df_reporters, 'new_designation', df_remove['original'], updated_name='new_designation')
cols = r3.replace(df_reporters, 'designation', df_jobs['original'], df_jobs['replacement'], df_remove['original'], redundancy = False)
df_reporters['new_designation'] = cols[0]
df_reporters['flags'] = cols[1]
df_reporters.to_csv('TestNew.csv', index = False)
print(df_reporters)