"""Bounded, no-follow remote TensorBoard event reader for SSH execution."""

REMOTE_EVENT_READER = r"""import base64,json,os,stat,sys
run,log=sys.argv[1:3]; cap=2*1024*1024; limit=256
if not run.startswith('/') or '..' in run.split('/') or not log or log.startswith('/') or '..' in log.split('/'): raise SystemExit(2)
fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY)
try:
 for part in run.split('/')[1:]+log.split('/'):
  n=os.open(part,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW,dir_fd=fd); os.close(fd); fd=n
 names=[]
 with os.scandir(fd) as entries:
  for i,e in enumerate(entries):
   if i>=limit: break
   if e.name.startswith('events.out.tfevents.') and e.is_file(follow_symlinks=False): names.append(e.name)
 if not names: raise SystemExit(3)
 f=os.open(sorted(names)[0],os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=fd)
 try:
  if not stat.S_ISREG(os.fstat(f).st_mode): raise SystemExit(4)
  data=os.read(f,cap)
 finally: os.close(f)
 print(json.dumps({'data':base64.b64encode(data).decode('ascii')}))
finally: os.close(fd)
"""
