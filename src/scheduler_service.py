from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime, timedelta
from config.settings import settings
from prediction import main

def job_notificaciones():
    print("Ejecutando job:", datetime.now())
    exe_date = datetime.today() #- timedelta(days=1)
    settings.EXECUTION_DAY = exe_date.strftime("%Y-%m-%d")
    print(settings.EXECUTION_DAY)
    main()

def job_notificaciones2():
    print("Ejecutando job:", datetime.now())
    main()

    print(settings.EXECUTION_DAY)
    exe_date = datetime.strptime(settings.EXECUTION_DAY,"%Y-%m-%d")
    exe_date = exe_date + timedelta(days=1)
    settings.EXECUTION_DAY = exe_date.strftime("%Y-%m-%d")
    print(settings.EXECUTION_DAY)
    if exe_date.day <= 26:
        job_notificaciones2()

#job_notificaciones()
#job_notificaciones2()

scheduler = BlockingScheduler()
scheduler.add_job(job_notificaciones , "cron", minute=0, hour = 12, misfire_grace_time= 300)
#scheduler.add_job(job_notificaciones , "cron", minute=40, hour = 15)

scheduler.start()