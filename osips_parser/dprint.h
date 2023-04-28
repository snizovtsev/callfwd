#ifndef OSIPS_DPRINT_H
#define OSIPS_DPRINT_H

#define LM_GEN2(facility, lev, ...) while (0) {}
#define LM_GEN1(lev, ...) while (0) {}
#define LM_ALERT( ...) while (0) {}
#define LM_CRIT( ...) while (0) {}
#define LM_ERR( ...) while (0) {}
#define LM_WARN( ...) while (0) {}
#define LM_NOTICE( ...) while (0) {}
#define LM_INFO( ...) while (0) {}
#define LM_DBG( ...) while (0) {}

#define LM_BUG(msg) \
	do { \
		LM_CRIT("\n>>> " msg"\nIt seems you have hit a programming bug.\n" \
				"Please help us make OpenSIPS better by reporting it at " \
				"https://github.com/OpenSIPS/opensips/issues\n\n"); \
	} while (0)

#endif // OSIPS_DPRINT_H
