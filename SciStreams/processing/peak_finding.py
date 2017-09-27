# -*- coding: utf-8 -*-
'''
The following code was authored by Jiliang Liu (jiliangliu@bnl.gov), and
imported into this library for convenience.

'''
import time
import numpy as np
import statsmodels.api as sm
import scipy.stats as stats
import matplotlib.pyplot as plt # for showing image
from pylab import * # for multiple figure window
import os
import scipy.io as sio
from lmfit import Model
import sys
from lmfit.models import GaussianModel, ExponentialModel, LorentzianModel, PolynomialModel
from scipy import signal,misc, ndimage
from skimage.draw import polygon
from skimage.draw import line
from scipy.interpolate import interp1d

class peak_finding:
    # TODO : make all kwargs explicit
    def __init__(self,intensity=None, **kwargs):
        self.limit_fev = kwargs.pop("limit_fev", False)
        if intensity is None:
            print ('not intensity input')
            sys,exit()
        else: self.intensity=intensity

        #set beam stop threshold
        self.beam_stop_effect = kwargs['beam_stop_effect'] if 'beam_stop_effect' in kwargs else 0
        #set end of intensity array
        self.end = kwargs['end'] if 'end' in kwargs else len(self.intensity)
        #set adjust_coefficient which determine the significancy of distinguable reflection. large adjust_coefficient only allow the algorithm to identify very sharp peaks
        self.adjust_coefficient = kwargs['adjust_coefficient'] if 'adjust_coefficient' in kwargs else 2
        #set fraction of the data for smooth
        self.frac = kwargs['frac'] if 'frac' in kwargs else 0.06/3
        #set iteration for smooth
        self.it = kwargs['it'] if 'it ' in kwargs else 5
        #set delta for local regression fitting, delta may introduced if furhter smooth or iteration needed.
        self.delta = kwargs['delta'] if 'delta' in kwargs else 0
        #set fraction of the data for over smooth, over smooth was enable an approximate setimation of background
        self.frac_over_smooth = kwargs['frac_over_smooth'] if 'frac_over_smooth' in kwargs else 1.8/3
        #set iteration for over smooth
        self.it_over_smooth = kwargs['it_over_smooth'] if 'it_over_smooth' in kwargs else 5
        #set the span used for background fitting, the span too small may introduce noise, but too large may lead to not efficient background estimation
        self.bkdg_span = kwargs['bkgd_span'] if 'bkgd_span' in kwargs else 20

    def peak_position(self,optim_bkgd=True,iter_num = 2,**kwargs):
        t0 = time.time()
        #asign value to variants
        intensity = np.copy(self.intensity)
        beam_stop_effect = self.beam_stop_effect
        frac = self.frac
        it = self.it
        delta = self.delta
        frac_over_smooth = self.frac_over_smooth
        it_over_smooth = self.it_over_smooth
        end = self.end
        #adjust_coefficient = self.adjust_coefficient


        x_origin= np.arange(0,len(intensity),1)
        if beam_stop_effect>0:
            intensity[0:beam_stop_effect]=nan
        else: pass
        intensity[intensity<-6]=nan
        y_origin = np.copy(intensity)
        not_nan_inds = np.where(isnan(y_origin)==0)[0]
        #return not_nan_inds
        if size(not_nan_inds)==0:
           return []
        else: pass
        if not_nan_inds[0]>beam_stop_effect:
            beam_stop_effect=not_nan_inds[0]
        if not_nan_inds[-1]<end:
            end=not_nan_inds[-1]
        #return end,x_origin,y_origin,beam_stop_effect
        #linear interpolate the missing point in middle of intensity curve
        if size(np.where(isnan(y_origin[beam_stop_effect:end])==1)[0])>0:
            fb = interp1d(x_origin[beam_stop_effect:end+1][np.where(isnan(y_origin[beam_stop_effect:end+1])==0)[0]], y_origin[beam_stop_effect:end+1][np.where(isnan(y_origin[beam_stop_effect:end+1])==0)[0]], kind='slinear')
            y_origin[beam_stop_effect:end+1] = fb(x_origin[beam_stop_effect:end+1])
        else: pass
        #nan should only appear at origin and end
        #not_nan_inds = np.arange(beam_stop_effect,end+1,1)
        not_nan_inds = np.where(isnan(y_origin)==0)[0]
        #y_origin had been interpolated, thus need new not_nan_ind
        x = x_origin[not_nan_inds]
        y = y_origin[not_nan_inds]
        z = sm.nonparametric.lowess(y,x, frac= frac_over_smooth,it=it_over_smooth)
        w = sm.nonparametric.lowess(y,x, frac= frac,it=it,delta=delta)
        #return w,intensity,y_origin,x_origin
        turn = np.sign(np.gradient(w[:,1],3))[0:-2]-np.sign(np.gradient(w[:,1],3))[1:-1]
        #turn = np.sign(np.gradient(y))[0:end-1]-np.sign(np.gradient(y))[1:end]
        local_minimum = np.where(turn<0)[0]
        normalized_gradient = np.gradient(w[:,1]/sum(w[:,1]))

        if len(normalized_gradient)<21.:
            b_c_width=len(normalized_gradient)
        else: b_c_width=21.

        #applied low pass filt, which enable to find the mean value of local region with width of 21 pixel and center at corresponding place
        low_pass_filt_gradient = np.convolve(normalized_gradient,np.ones((int(b_c_width),))/ b_c_width,'same')
        #standard deviation of local region is calculated by invovling a convolvution of boxfunction with width of 21 pixel
        gradient_local_std = np.convolve(np.abs(normalized_gradient-low_pass_filt_gradient),np.ones((int(b_c_width),))/ b_c_width,'same')
        #eliminate the incorrect local minimum
        #local_minimum = local_minimum[np.where(gradient_local_std[local_minimum]<np.sum(gradient_local_std)/len(gradient_local_std))[0]]
        #
        flat_region = np.where(gradient_local_std<.005*np.sum(gradient_local_std)/len(gradient_local_std))[0]
        #1e-6 is applied to determine the flat rate of data, it is a empirical parame˙ter here
        local_minimum = np.unique(np.append(local_minimum,flat_region))
        if size(local_minimum)==0:
            return []
        else: pass
        turn_minimum_exist = False
        if local_minimum[0] > 0:
            if w[local_minimum[0],1] < w[0,1]:
                local_minimum = np.append(np.arange(0,local_minimum[0],1),local_minimum)
            else:
                turn_minimum_exist = True
                turn_minimum = np.where(np.diff(np.sign(np.gradient(gradient_local_std)))>0)[0]
                local_minimum = np.append(np.arange(0,turn_minimum[0],1),local_minimum)#actually this may cause an additional unexpect peak between first peak and beam stop
        #return turn_minimum_exist,turn_minimum
        #if local_minimum[0]>beam_stop_effect:
        #    local_minimum = np.append(beam_stop_effect,local_minimum)

        #find local minimum and cubic spline the local minimum as estimated background
        local_minimum = w[local_minimum,0].astype(int)
        if local_minimum[-1]<w[-1,0]:
            local_minimum = np.append(local_minimum,np.arange(local_minimum[-1]+1,int(w[-1,0])+1,1))

        #local_minimum = local_minimum+beam_stop_effect
        bkgd = np.zeros((len(y_origin),))*nan

        y_origin[w[:,0].astype(int)] = w[:,1]
        #return y,y_origin,local_minimum,w[:,1],intensity,not_nan_inds,w[:,0]
        f2 = interp1d(local_minimum, y_origin[local_minimum], kind='slinear')
        bkgd[local_minimum[0]:local_minimum[-1]] = f2(np.arange(local_minimum[0],local_minimum[-1],1))
        bkgd[np.where(y_origin-bkgd<0)[0]]=y_origin[np.where(y_origin-bkgd<0)[0]]

        #return w,y_origin,bkgd,x_origin,local_minimum

        optim_bkgd=optim_bkgd
        iter_num = 2
        if len(normalized_gradient)<81.:
            b_c_width=len(normalized_gradient)
        else: b_c_width=81.
        #b_c_width is background convolve window width, if window is too narrow, will not be able distinguish sharp (which is wanted to eliminated) and diffuse background
        if optim_bkgd==True:
            bkgd_gradient = np.gradient(bkgd[beam_stop_effect:end]/np.sum(bkgd[beam_stop_effect:end]),3)
            local_ave_gradient = np.convolve(bkgd_gradient,np.ones((int(b_c_width),))/b_c_width,'same')
            local_std_gradeint = np.convolve(np.abs(bkgd_gradient-local_ave_gradient),np.ones((int(b_c_width),))/b_c_width,'same')
            bkgd_turn = np.sign(bkgd_gradient)[0:-2]-np.sign(bkgd_gradient)[1:-1]
            bkgd_local_minimum = np.where(bkgd_turn<0)[0]+beam_stop_effect
            if turn_minimum_exist == True:
                bkgd_local_minimum = np.append(turn_minimum[0]+beam_stop_effect,bkgd_local_minimum)
            for g in range(iter_num):
                bkgd_gradient = np.gradient(bkgd[beam_stop_effect:end]/np.sum(bkgd[beam_stop_effect:end]),3)
                local_ave_gradient = np.convolve(bkgd_gradient,np.ones((int(b_c_width),))/b_c_width,'same')
                local_std_gradeint = np.convolve(np.abs(bkgd_gradient-local_ave_gradient),np.ones((int(b_c_width),))/b_c_width,'same')
                #return local_std_gradeint,local_ave_gradient
                if size(bkgd_local_minimum)>0:
                    #return bkgd_local_minimum
                    bkgd_judge = np.sum(local_std_gradeint[bkgd_local_minimum[0]:-1])/len(local_std_gradeint[bkgd_local_minimum[0]:-1])
                    for mini_bkgd in range(len(bkgd_local_minimum)-1):
                        local_mini_std = np.sum(local_std_gradeint[bkgd_local_minimum[mini_bkgd]:bkgd_local_minimum[mini_bkgd+1]])/len(local_std_gradeint[bkgd_local_minimum[mini_bkgd]:bkgd_local_minimum[mini_bkgd+1]])
                        if local_mini_std > .6*bkgd_judge:
                            f1 = interp1d(np.asarray([bkgd_local_minimum[mini_bkgd],bkgd_local_minimum[mini_bkgd+1]]), np.asarray([bkgd[bkgd_local_minimum[mini_bkgd]],bkgd[bkgd_local_minimum[mini_bkgd+1]]]), kind='slinear')
                            bkgd[bkgd_local_minimum[mini_bkgd]:bkgd_local_minimum[mini_bkgd+1]] = f1(np.arange(bkgd_local_minimum[mini_bkgd],bkgd_local_minimum[mini_bkgd+1],1))
                    bkgd[np.where(y_origin-bkgd<0)[0]]=y_origin[np.where(y_origin-bkgd<0)[0]]
                else:pass
        variance = y_origin[beam_stop_effect:end]-bkgd[beam_stop_effect:end]
        variance_mean = np.sum(abs(y_origin[beam_stop_effect:end]-bkgd[beam_stop_effect:end]))/len(x_origin[beam_stop_effect:end])
        #here to determine whether the peak is too weak to consider as a reflection from sample, should have intensity large than 2
        #return w,y_origin,bkgd,x_origin
        intensity_threshold=-1
        if size(np.where(variance>intensity_threshold)[0])==0:
            return []
        else: pass
        '''if np.sum(variance[np.where(variance>variance_mean)[0]])/np.sum(bkgd[beam_stop_effect:end][np.where(variance>variance_mean)[0]])<.1:
            return []
        else: pass'''

        '''if min(variance)<0:
            #bkgd = bkgd+min(variance)
            variance = variance-min(variance)#variance had been reassign the value'''

        turn_v = np.sign(np.gradient(variance,3))[0:-2]-np.sign(np.gradient(variance,3))[1:-1]
        inds=np.asarray(np.where(turn_v>0))
        #return inds
        #return variance,variance_mean,inds
        inds_peak = list()
        for num1 in range(len(inds[0,:])):
            if variance[inds[0,num1]]>self.adjust_coefficient*variance_mean:
                inds_peak.append(inds[0,num1])
                #print inds[0,num1]
            elif variance[inds[0,num1]+1]>self.adjust_coefficient*variance_mean:
                 inds_peak.append(inds[0,num1]+1)
            #print inds[0,num1]+1
            #else: inds_peak = inds_peak
        #print x[np.unique(np.asarray(inds_peak))+beam_stop_effect]
        #x=np.copy(x_origin[beam_stop_effect:end])
        #x=x[isnan(variance)==0]
        #'''
        #here x should relate to variance not only intensity not nan, there is interpolation for variance
        #'''
        if size(inds_peak)==0:
            return inds_peak
        else:
            inds_peak_amp = inds_peak
            inds_peak = x[np.unique(np.asarray(inds_peak))]
           #return inds_peak,variance,variance_mean,y,bkgd,turn_minimum_exist
        #np.round assume distance between two neighbor peaks need to be larger 1 pixel

        xdata=x_origin[beam_stop_effect:end]
        for num11 in range(len(inds_peak)):
               #print num11
               pre_name = "g%d_" % (num11+1)
               pre_name_center =  "g%d_center" % (num11+1)
               pre_name_sigma =  "g%d_sigma" % (num11+1)
               pre_name_amplitude =  "g%d_amplitude" % (num11+1)
               #pre_name_height =  "g%d_height" % (num11+1)
               if num11 == 0:
                   mod = GaussianModel(prefix=pre_name)
                   pars =  GaussianModel(prefix=pre_name).make_params()
               else:
                   pars.update( GaussianModel(prefix=pre_name).make_params())
                   mod += GaussianModel(prefix=pre_name)
               pars[pre_name_center].set(inds_peak[num11],min=inds_peak[num11]-2,max=inds_peak[num11]+2)#limit the peak position shift to ensure the fitting accuracy
               pars[pre_name_sigma].set(.5,min=0.001) # very native and intuitive guess
               pars[pre_name_amplitude].set(variance[inds_peak_amp[num11]],min=0) # amplitude guess from intensity of inds_peak
        #print("Now fitting")
        #print("model : {}".format(mod))
        #raise ValueError
        # 10,000 func calls is max
        # JULIEN ADDED THIS: put a cap on the number of function evaluations.
        # This part seems to take forever if the number of peaks is large
        # Also, if num peaks is large, maybe this is a mistake?
        # Or alternatively, if the number of peaks is large, perhaps we should
        # try to see which ones we might want to pick that seem more likely to
        # be peaks than others by some criterion?
        if self.limit_fev:
            maxevals = 1000
            # number of parameters
            N = len(mod.param_names)
            maxfev = np.minimum(maxevals, 200*(N+1))
            fit_kws = dict(maxfev=maxfev)
        else:
            fit_kws = dict()

        out = mod.fit(variance, pars, x=xdata, method='leastsq',fit_kws=fit_kws)

        #out = mod.fit(w[beam_stop_effect:end,1], pars, x=xdata)
        '''for num12 in range(len(inds_peak)):
            figure(1)
            plt.plot(xdata,out.eval_components().items()[num12+1][1]+\
            out.eval_components().items()[0][1],linewidth=1.0)
            plt.show()

        for num11 in range(len(inds_peak)):
            pre_name_center =  "g%d_center" % (num11+1)
            pre_name_fwhm =  "g%d_fwhm" % (num11+1)
            print pre_name_center+':'+' '+np.str(out.params[pre_name_center].value)
            print pre_name_fwhm+':'+' '+np.str(out.params[pre_name_fwhm].value)'''

        #return inds_peak,out,turn,x,xdata,y
        ratio=np.sum(variance[np.where(variance>variance_mean)[0]])/np.sum(bkgd[beam_stop_effect:end][np.where(variance>variance_mean)[0]])
        return out,y_origin[beam_stop_effect:end],inds_peak,xdata,ratio,z[:,1],y,w,bkgd,variance,variance_mean#,gradient_local_std,low_pass_filt_gradient
