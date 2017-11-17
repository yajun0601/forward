#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  9 10:23:07 2017

@author: yajun
"""
def my_print(content):
    print('my_print:',content)

def send_mail(mailto_list, subject, output_dir, attachment, content):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.application import MIMEApplication

    mail_host="smtp.exmail.qq.com"
    #设置服务器
    mail_user="zhengyajun"
    #用户名
    mail_pass="Forward2017"
    #口令
    mail_postfix="zhengqf.com"
    #发件箱的后缀
    me="郑亚军"+"<"+mail_user+"@"+mail_postfix+">"
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = me
    msg['To'] = ",".join(mailto_list)
    
    puretext = MIMEText("新增的信息:\n\n")
    msg.attach(puretext)
        
    puretext = MIMEText(content)
    msg.attach(puretext)     
    
    # xlsx类型的附件
    xlsxpart = MIMEApplication(open(attachment, 'rb').read())
    xlsxpart.add_header('Content-Disposition', 'attachment', filename = attachment)
    msg.attach(xlsxpart)
    
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.login(mail_user+"@"+mail_postfix,mail_pass)
        server.sendmail(me, mailto_list, msg.as_string())
        server.close()
        return True
    except Exception as e:
        print (str(e))
        return False