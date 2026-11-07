import argparse
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from backend.database import seed, context, DB
p=argparse.ArgumentParser(description='Generate synthetic fleet records into a NEW database')
p.add_argument('--days',type=int,default=14)
p.add_argument('--vehicles',type=int,default=300)
p.add_argument('--output',type=Path,default=DB)
args=p.parse_args()
if args.days<1 or args.vehicles<6: p.error('days >= 1 and vehicles >= 6 required')
seed(args.output,args.days,args.vehicles)
for table,meta in context(args.output).items(): print(table,meta['rows'])
