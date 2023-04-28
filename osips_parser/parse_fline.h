/*
 * Copyright (C) 2001-2003 FhG Fokus
 *
 * This file is part of opensips, a free SIP server.
 *
 * opensips is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version
 *
 * opensips is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA
 */


#ifndef PARSE_FLINE_H
#define PARSE_FLINE_H

#include "str.h"

/* Message is request */
#define SIP_REQUEST 1

/* Message is reply */
#define SIP_REPLY   2

/* Invalid message */
#define SIP_INVALID 0

#define SIP_VERSION "SIP/2.0"
#define SIP_VERSION_LEN 7

struct msg_start {
	int type;                 /* Type of the Message - Request/Response */
	int len;                  /* length including delimiter */
	union {
		struct {
			str method;       /* Method string */
			str uri;          /* Request URI */
			str version;      /* SIP version */
			int method_value;
		} request;
		struct {
			str version;      /* SIP version */
			str status;       /* Reply status */
			str reason;       /* Reply reason phrase */
			unsigned int /* statusclass,*/ statuscode;
		} reply;
	}u;
};


char* parse_first_line(char* buffer, unsigned int len, struct msg_start * fl);

char* parse_fline(char* buffer, char* end, struct msg_start* fl);


#endif /* PARSE_FLINE_H */
