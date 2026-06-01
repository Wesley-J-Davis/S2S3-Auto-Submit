#!/usr/bin/env python

import argparse
import datetime
import os
import sys

def main():

   # Set founds to False
   # -------------------
    
   CMAP_found   = False
   anaeta_found = False
   OSTIA_found  = False
   rccode       = 0

   # Parse arguments
   # ---------------

   args    = parse_args()
   year    = args['year']
   month   = args['month']
   day     = args['day']
   verbose = args['verbose']

   # Create our input day datetime object
   # ------------------------------------

   inputday = datetime.datetime(year=year,month=month,day=day)

   # Check for CMAP file existence
   # -----------------------------

   CMAP_path   = "/discover/nobackup/dao_ops/PrecipCorr/CMAPcorr"
   CMAP_prefix = "d5124_rpit_jan12.tavg1_2d_lfo_Nx_CMAPcorr."
   CMAP_suffix = ".nc4"

   CMAP_target_time = inputday.replace(hour=23,minute=30)
   CMAP_target_time = CMAP_target_time - datetime.timedelta(days=1)
   CMAP_target_time_string = CMAP_target_time.strftime("%Y%m%d_%H%Mz")
   CMAP_filename = CMAP_prefix + CMAP_target_time_string + CMAP_suffix
   CMAP_file = os.path.join(os.path.sep, CMAP_path, CMAP_filename)

   if verbose: print("Looking for", CMAP_file,)

   if os.path.isfile(CMAP_file):
      if verbose: print("...Found!")
      CMAP_found = True
   else:
      if verbose: print("...NOT FOUND")
      rccode += 1

   # Check for ana.eta file existence
   # --------------------------------

   anaeta_path   = "/discover/nobackup/dao_ops/scratch/d5124_rpit_jan12/ana"
   anaeta_prefix = "d5124_rpit_jan12.ana.eta."
   anaeta_suffix = ".nc4"

   anaeta_target_time = inputday.replace(hour=18)
   anaeta_target_time_string = anaeta_target_time.strftime("%Y%m%d_%Hz")

   anaeta_target_year_string = anaeta_target_time.strftime("Y%Y")
   anaeta_target_month_string = anaeta_target_time.strftime("M%m")

   anaeta_filename = anaeta_prefix + anaeta_target_time_string + anaeta_suffix

   anaeta_file = os.path.join(os.path.sep,
         anaeta_path, 
         anaeta_target_year_string, 
         anaeta_target_month_string,
         anaeta_filename)

   if verbose: print("Looking for", anaeta_file,)

   if os.path.isfile(anaeta_file):
      if verbose: print("...Found!")
      anaeta_found = True
   else:
      if verbose: print("...NOT FOUND")
      rccode += 10 

   # Check for OSTIA completion
   # --------------------------

   OSTIA_file = "/home/dao_ops/D_BOSS/schedule/files/discover36/task_status"

   OSTIA_target_time = inputday
   #OSTIA_target_time = inputday - datetime.timedelta(days=1)
   OSTIA_target_time_string = OSTIA_target_time.strftime("%H:%M")
   OSTIA_target_date_string = OSTIA_target_time.strftime("%Y-%m-%d")

   OSTIA_string = ", ".join(["QUART-OSTIA-REYNOLDS-01",
                             OSTIA_target_time_string ,
                             OSTIA_target_date_string ,
                             "COMPLETE"])

   if verbose: print("Looking for", OSTIA_string, "in", OSTIA_file,)

   if os.path.isfile(OSTIA_file):
      for line in open(OSTIA_file):
         if OSTIA_string in line:
            if verbose: print("...Found!")
            OSTIA_found = True

   if not OSTIA_found:
      if verbose: print("...NOT FOUND")
      rccode += 100

   if verbose: print("rccode: ", str(rccode).zfill(3))

   if CMAP_found and anaeta_found and OSTIA_found:
      sys.exit(0)
   else:
      sys.exit(rccode)


def parse_args():
    
   p = argparse.ArgumentParser(description='Date checker for S2S')

   requiredNamed = p.add_argument_group('required named arguments')

   requiredNamed.add_argument('--year',  type=int, help='Year',  default=None, required=True)
   requiredNamed.add_argument('--month', type=int, help='Month', default=None, required=True)
   requiredNamed.add_argument('--day',   type=int, help='Day',   default=None, required=True)

   p.add_argument('--verbose', help="Verbose", action='store_true')

   return vars(p.parse_args())

if __name__=="__main__":
   main()
