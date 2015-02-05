#!/usr/local/bin/python
#---------------------------------------------------------------------------------------------------
# Access CRAB3 data via HTCondor
#---------------------------------------------------------------------------------------------------
import sys, os, htcondor, classad, datetime, ConfigParser
from email.MIMEText import MIMEText
from email.MIMEMultipart import MIMEMultipart
from email.Utils import formataddr
from subprocess import Popen, PIPE

class crabApi():
    def __init__(self):
        config = ConfigParser.RawConfigParser()
        config.read('/usr/local/IntelROCCS/DataDealer/intelroccs.cfg')
        collectorName = config.get('CRAB', 'collector')
        for attempt in range(3):
            try:
                collector = htcondor.Collector(collectorName)
                self.schedulers = collector.locateAll(htcondor.DaemonTypes.Schedd)
            except IOError, e:
                continue
            else:
                break
        else:
            self.error(e)

    def error(self, e):
        title = "FATAL IntelROCCS Error -- CRAB"
        text = "FATAL -- %s" % (str(e),)
        fromEmail = ("Bjorn Barrefors", "bjorn.peter.barrefors@cern.ch")
        toList = (["Bjorn Barrefors"], ["barrefors@gmail.com"])
        msg = MIMEMultipart()
        msg['Subject'] = title
        msg['From'] = formataddr(fromEmail)
        msg['To'] = self._toStr(toList)
        msg1 = MIMEMultipart("alternative")
        msgText1 = MIMEText("<pre>%s</pre>" % text, "html")
        msgText2 = MIMEText(text)
        msg1.attach(msgText2)
        msg1.attach(msgText1)
        msg.attach(msg1)
        msg = msg.as_string()
        p = Popen(["/usr/sbin/sendmail", "-toi"], stdin=PIPE)
        p.communicate(msg)
        raise Exception("FATAL -- %s" % (str(e),))

    def _toStr(self, toList):
        names = [formataddr(i) for i in zip(*toList)]
        return ', '.join(names)

#===================================================================================================
#  M A I N   F U N C T I O N
#===================================================================================================
    def crabCall(self, query, attributes=[]):
        data = []
        for scheduler in self.schedulers:
            for attempt in range(3):
                try:
                    schedd = htcondor.Schedd(scheduler)
                    jobs = schedd.query(query, attributes)
                    for job in jobs:
                        data.append(job)
                except IOError, e:
                    continue
                else:
                    break
            else:
                self.error(e)
        return data