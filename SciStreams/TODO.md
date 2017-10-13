Todo:
- maybe remove data from xml saving?
- Add detector to folder name
- make sure directory structrure sam eas /GPFS
- add loop listening for data
- remove all IP addresses from code, replace with a client.yml or something else
- client model
	https://nsls-ii.github.io/bluesky/callbacks.html#subscriptions-in-separate-processes-or-host-with-0mq
- Overall interface:
    - Better filling of the plot area
    - Labelling axes with the variable and the unit (e.g. in a 1D plot there 
    is a difference between showing 'counts' and 'counts/pixel').
    - Smarter selection of defaults and scaling ranges
    	- E.g. logscale is usually useful for the circular average; some 
    		clipping of the zscale helps for area plots; etc.
    - Putting on helpful additional information
      - This is very 'graph specific', but I'm thinking of things like for 
    		q-axes (1D plots) one could define an upper/alternate axis that is 
    		2*pi/q as a useful .
    	- Doing some simple fits
    	- In 1D data if there's an 'obvious' peak, then fit it and label the 
    		position
			- If it's an angular cut then try fitting to a simple periodic
			  function...
    - Better stacking behavior for things like "linecuts-axisy" (I don't 
    		know what the "value: ..." is, but it obscures much of the data.
		- more intuitive way to make plots (multiply by q^2 or q^1.5 etc)
		- change color map to make images more visible?
