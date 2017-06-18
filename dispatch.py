# testing some closures  

def flip():
	n = 0
	while True:
		n = bool((n + 1) % 2)
		yield n
		
def switch(n=None):
	'''iterates as an endless loop.'''
	is_flipswitch = isinstance(n, bool)
	class Rotator:
		needle, spinner = (abs(n-1), 2) if is_flipswitch else (-1, n)

	if is_flipswitch:
		def inner():
			Rotator.needle = (Rotator.needle + 1) % Rotator.spinner
			return bool(Rotator.needle)
	else:
		def inner():
			Rotator.needle = (Rotator.needle + 1) % Rotator.spinner
			return Rotator.needle + 1
	return inner
	
