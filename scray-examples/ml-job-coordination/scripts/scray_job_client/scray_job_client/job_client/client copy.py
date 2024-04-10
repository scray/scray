#!/usr/bin/env python
# coding: utf-8

# In[1]:


### add imports for uploader release 0.1

import os
from job import *



# In[3]:


import gradio as gr
gr.close_all()


# #### upload (uploader)

# In[4]:


from pathlib import Path
import time
import os

def process_tempfile(path,input_file,topics,project,version, ip_address,bucket=None):
    try:
        if len(topics) == 0:
            topics = 'unknown'
        topics_list = [topic.strip() for topic in topics.split(';') if len(topic.strip()) > 0]
        metadata = upload_file_and_create_job(client=scray_job_client,file_path=path,topics=topics_list,project=project,version=version,filename=input_file.split('/')[-1],ip_address = ip_address,bucket=bucket) 
        
        return str(metadata)
    except Exception as e:
        return str(e)

def process_tempfiles(file_paths,topics,project,version,ip_address,bucket=None):
    try:
        for file in file_paths:
            path=Path(str(file))
            metadata = process_tempfile(path,file,topics,project,version,ip_address,bucket=bucket)
            ## FIXME delete tmp file
            os.remove(path)
        return metadata    
    except Exception as e:
        return str(e)

def upload_local_file(files, topics,project,version,bucket, request : gr.Request):

    global input
    file_paths = [file.name for file in files]

    ip_address = request.client.host
    input = process_tempfiles(file_paths,topics,project,version,ip_address,bucket=bucket)
    
    return file_paths

"""
def process_textfields(t1,t2):
    return t1 + t2
"""

def process_tempfile_gradio(input_file,topics,project, request: gr.Request):
    try:
        ip_address = request.client.host
        path=Path(str( input_file.name))
        if len(topics) == 0:
            topics = 'unknown'
        topics_list = [topic.strip() for topic in topics.split(';') if len(topic.strip()) > 0]
        metadata = upload_file_and_create_job(client=scray_job_client,file_path=path,topics=topics_list,project=project,filename=input_file.name.split('/')[-1],ip_address = ip_address) 
        ## FIXME delete tmp file
        return str(metadata)
    except Exception as e:
        return str(e)



# #### jobs (uploader)

# In[5]:


def get_process_status():
    jobs_ready_to_convert = scray_job_client.get_jobs(processing_env="http://scray.org/ai/app/env/see/os/k8s", requested_state="READY_TO_CONVERT")
    jobs_converted = scray_job_client.get_jobs(processing_env="http://scray.org/ai/app/env/see/os/k8s", requested_state="CONVERTED")
    return str(jobs_ready_to_convert), str(jobs_converted)


# In[6]:


#metadata
#input


# In[7]:


DESCRIPTION_APP = """
<p style="text-align: center;">
    <img src="file/SEEBURGER-Logo-RGB.svg" alt="SEEBURGER AG Logo" width="150" height="25" style="display: inline-block; vertical-align: middle;">
</p>
"""

FOOTER = """
        <!-- Roter Banner -->
        <div class="red-banner">
                      
          <a href="https://www.seeburger.com/legal-notice/">Legal Notice</a> | <a href="https://www.seeburger.com/terms-of-use/">Terms of Use</a> | <a href="https://www.seeburger.com/copyright/">Copyright</a> | <a href="https://www.seeburger.com/data-privacy-policy/">Data Privacy Policy</a> | <a href="https://www.seeburger.com/compliance/">Compliance</a> 
        </div>
"""


# In[8]:


_bucket = 'test5-data'
def set_bucket(bucket = None):
    global _bucket
    _bucket = bucket

def get_bucket():
    return _bucket


# In[9]:


with gr.Blocks(title='uploader') as demo:
    bucket_tb = gr.Textbox(
            value=get_bucket(),
            label='bucket (server)',
            placeholder='database',
            container=True,
            show_label=True
        )   
    bucket_tb.change(set_bucket,bucket_tb,None)

    with gr.Accordion(label='Extendet Client', open=True, visible=True):
        with gr.Row():
            project_tb = gr.Textbox(
                container=False,
                show_label=False,
                placeholder='Project Name    ...     <<ENTER>>',
                scale=10,
            )
            gr.Markdown(DESCRIPTION_APP)
    
        
        
        gr.Markdown("""# Upload a file or record some audio <BR> formats: .md, .pdf, .txt, audio: (.wav, .mp3, .flac) , video: (.mp4'), AsciiDoc: (.zip) 
        """)
    
        # known tags
        def process_example_tags(tag, taglist):
            return taglist + ' ; ' + tag + ' ; '
        main_tags = ['API_Management', 'Mappings', 'MFT', 'Security','S/4HANA', 'Cloud', 'Test']
        type_tags = ['documentation','expert talk','training', 'webcast','mte','blog', 'customer story', 'changelog']
        other_tags = ['generic','collateral','analysts', 'product', 'legal']
        tag_tb = gr.Textbox(value='',visible=False)
        example_tags_ex = gr.Examples(examples=main_tags,
                            label='main',   
                            inputs=tag_tb,
                            #outputs=[tag_tb],
                            fn=lambda x:x,
                            cache_examples=False,
                            )
        example_tags_2_ex = gr.Examples(examples=type_tags,
                            label='type',   
                            inputs=tag_tb,
                            #outputs=[tag_tb],
                            fn=lambda x:x,
                            cache_examples=False,
                            )
        example_tags_3_ex = gr.Examples(examples=other_tags,
                            label='other',   
                            inputs=tag_tb,
                            #outputs=[tag_tb],
                            fn=lambda x:x,
                            cache_examples=False,
                            )
        
        file_topics_tb = gr.Textbox(
            interactive=True,
            placeholder="""            Mappings; training; webcast; ...
            
            Hint: add tags describing the documents (optional)""",
            container=False,
            show_label=False,
            scale=10,
            lines=3,
        )   
    
        tag_tb.change(process_example_tags,inputs=[tag_tb,file_topics_tb],outputs=file_topics_tb)    
    
        version_tb = gr.Textbox(
                        label='Version',
                        container=True,
                        show_label=True,
                        placeholder='6.7, new, ...                                Hint: [A-Za-z0-9_-.,:§$%&/|><]*  (optional)',
                        scale=10,
                        lines=1,
                    ) 
        
        #file_output = gr.File(visible=True)
        #progress=gr.Progress(value = 0.2)
        upload_button = gr.UploadButton("Click to Upload Files (multiple)", file_types=["file"], file_count="multiple")
    
        with gr.Accordion(label='Upload Status', open=True, visible=True):
            #file_output = gr.File()
            file_upload_tb = gr.Textbox(
                    container=False,
                    show_label=False,
                    placeholder='Response...',
                    scale=10,
                    lines=6,
                )       
            """
            file_metadata_tb = gr.Textbox(
                        container=False,
                        show_label=False,
                        scale=10,
                        lines=1,
                    )   
            """     
            file_status_1_tb = gr.Textbox(
                        label='READY_TO_CONVERT',
                        container=True,
                        show_label=True,
                        scale=10,
                        lines=3,
                    )    
            file_status_2_tb = gr.Textbox(
                        label='CONVERTED',
                        container=True,
                        show_label=True,
                        scale=10,
                        lines=3,
                    )     
    
            """
            file_status_3_tb = gr.Textbox(
                        label='Status',
                        container=True,
                        show_label=True,
                        scale=10,
                        lines=3,
                    )     
            """
            
            #file_status_df = with gr.Row(visible = True):   
            generated_statements_df = gr.Dataframe(
                value = get_ask_jobs_value_list(),
                headers = ['start','s_id','project','tags','filename','status']
            )   
            active_jobs_bt = gr.Button(value='Update Active Jobs', variant='secondary')
    
            completed_jobs_df = gr.Dataframe(
                value = get_completed_ask_jobs_value_list(),
                headers = ['start','s_id','project','tags','filename','status']
            )   
            
    
            available_documents_tb = gr.Textbox(
                        label='Documents available in Collection',
                        container=True,
                        show_label=True,
                        scale=10,
                        lines=3,
                    )    
            
            completed_jobs_bt = gr.Button(value='Update Completed Jobs', variant='secondary')
        
        """ def output_txt():
            return 'upload' """
        #upload_button.click(output_txt,None,file_upload_tb)
        
        
        upload_button.upload(upload_local_file, inputs=[upload_button,file_topics_tb,project_tb,version_tb,bucket_tb], outputs = file_upload_tb
                          
        #upload_button.upload(upload_local_file, inputs=[upload_button,file_topics_tb,project_tb], outputs = file_output                     
        #).then(process_tempfile, inputs = [file_output,file_topics_tb,project_tb],outputs=[file_metadata_tb]
        ).then(get_process_status, inputs=[], outputs = [file_status_1_tb,file_status_2_tb]
        #).then(get_ask_jobs_str, inputs=None, outputs = file_status_3_tb
        ).then(get_ask_jobs_value_list, inputs=None, outputs = generated_statements_df)        
    
        file_status__button_event = active_jobs_bt.click(get_process_status, inputs=[], outputs = [file_status_1_tb,file_status_2_tb]
                                                             ).then(get_ask_jobs_value_list, inputs=None, outputs = generated_statements_df)   
                                                        
        completed_jobs_event = completed_jobs_bt.click(get_completed_ask_jobs_value_list, inputs=None, outputs = completed_jobs_df
                                                      ).then(get_available_documents,None,available_documents_tb)


    with gr.Accordion(label='API', open=False, visible=True):
        bucket_return_tb = gr.Textbox(label='Return Bucket')
        bucket_return_bt = gr.Button(value='Return Bucket',visible=True)
        bucket_return_bt.click(get_bucket,inputs=None,outputs=bucket_return_tb,api_name='bucket')

    gr.Markdown(FOOTER)

demo.queue(max_size=20).queue(concurrency_count=5, max_size=20).launch(server_name="0.0.0.0")
