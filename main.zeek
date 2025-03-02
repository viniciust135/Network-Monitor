##! The Zeek logging interface.
##!
##! See :doc:`/frameworks/logging` for an introduction to Zeek's
##! logging framework.

module Log;

export {
	## Type that defines an ID unique to each log stream. Scripts creating new
	## log streams need to redef this enum to add their own specific log ID.
	## The log ID implicitly determines the default name of the generated log
	## file.
	type Log::ID: enum {
		## Dummy place-holder.
		UNKNOWN
	};

	## If true, local logging is by default enabled for all filters.
	const enable_local_logging = T &redef;

	## If true, remote logging is by default enabled for all filters.
	const enable_remote_logging = T &redef;

	## Default writer to use if a filter does not specify anything else.
	#const default_writer = WRITER_ASCII &redef;
	const default_writer = WRITER_NONE &redef;

	## Default separator to use between fields.
	## Individual writers can use a different value.
	const separator = "\t" &redef;

	## Default separator to use between elements of a set.
	## Individual writers can use a different value.
	const set_separator = "," &redef;

	## Default string to use for empty fields. This should be different
	## from *unset_field* to make the output unambiguous.
	## Individual writers can use a different value.
	const empty_field = "(empty)" &redef;

	## Default string to use for an unset &optional field.
	## Individual writers can use a different value.
	const unset_field = "-" &redef;

	## Type defining the content of a logging stream.
	type Stream: record {
		## A record type defining the log's columns.
		columns: any;

		## Event that will be raised once for each log entry.
		## The event receives a single same parameter, an instance of
		## type ``columns``.
		ev: any &optional;

		## A path that will be inherited by any filters added to the
		## stream which do not already specify their own path.
		path: string &optional;
	};

	## Builds the default path values for log filters if not otherwise
	## specified by a filter. The default implementation uses *id*
	## to derive a name.  Upon adding a filter to a stream, if neither
	## ``path`` nor ``path_func`` is explicitly set by them, then
	## this function is used as the ``path_func``.
	##
	## id: The ID associated with the log stream.
	##
	## path: A suggested path value, which may be either the filter's
	##       ``path`` if defined, else a previous result from the function.
	##       If no ``path`` is defined for the filter, then the first call
	##       to the function will contain an empty string.
	##
	## rec: An instance of the stream's ``columns`` type with its
	##      fields set to the values to be logged.
	##
	## Returns: The path to be used for the filter.
	global default_path_func: function(id: ID, path: string, rec: any) : string &redef;

	# Log rotation support.

	## Information passed into rotation callback functions.
	type RotationInfo: record {
		writer: Writer;		##< The log writer being used.
		fname: string;		##< Full name of the rotated file.
		path: string;		##< Original path value.
		open: time;		##< Time when opened.
		close: time;		##< Time when closed.
		terminating: bool;	##< True if rotation occured due to Zeek shutting down.
	};

	## Default rotation interval to use for filters that do not specify
	## an interval. Zero disables rotation.
	##
	## Note that this is overridden by the ZeekControl LogRotationInterval
	## option.
	const default_rotation_interval = 0secs &redef;

	## Default naming format for timestamps embedded into filenames.
	## Uses a ``strftime()`` style.
	const default_rotation_date_format = "%Y-%m-%d-%H-%M-%S" &redef;

	## Default shell command to run on rotated files. Empty for none.
	const default_rotation_postprocessor_cmd = "" &redef;

	## Specifies the default postprocessor function per writer type.
	## Entries in this table are initialized by each writer type.
	const default_rotation_postprocessors: table[Writer] of function(info: RotationInfo) : bool &redef;

	## Default alarm summary mail interval. Zero disables alarm summary
	## mails.
	##
	## Note that this is overridden by the ZeekControl MailAlarmsInterval
	## option.
	const default_mail_alarms_interval = 0secs &redef;

	## Default field name mapping for renaming fields in a logging framework
	## filter.  This is typically used to ease integration with external
	## data storage and analysis systems.
	const default_field_name_map: table[string] of string = table() &redef;

	## Default separator for log field scopes when logs are unrolled and
	## flattened.  This will be the string between field name components.
	## For example, setting this to "_" will cause the typical field
	## "id.orig_h" to turn into "id_orig_h".
	const default_scope_sep = "." &redef;

	## A prefix for extension fields which can be optionally prefixed
	## on all log lines by setting the `ext_func` field in the
	## log filter.
	const Log::default_ext_prefix: string = "_" &redef;

	## Default log extension function in the case that you would like to
	## apply the same extensions to all logs.  The function *must* return
	## a record with all of the fields to be included in the log. The
	## default function included here does not return a value, which indicates
	## that no extensions are added.
	const Log::default_ext_func: function(path: string): any =
		function(path: string) { } &redef;

	## A filter type describes how to customize logging streams.
	type Filter: record {
		## Descriptive name to reference this filter.
		name: string;

		## The logging writer implementation to use.
		writer: Writer &default=default_writer;

		## Indicates whether a log entry should be recorded.
		## If not given, all entries are recorded.
		##
		## rec: An instance of the stream's ``columns`` type with its
		##      fields set to the values to be logged.
		##
		## Returns: True if the entry is to be recorded.
		pred: function(rec: any): bool &optional;

		## Output path for recording entries matching this
		## filter.
		##
		## The specific interpretation of the string is up to the
		## logging writer, and may for example be the destination
		## file name. Generally, filenames are expected to be given
		## without any extensions; writers will add appropriate
		## extensions automatically.
		##
		## If this path is found to conflict with another filter's
		## for the same writer type, it is automatically corrected
		## by appending "-N", where N is the smallest integer greater
		## or equal to 2 that allows the corrected path name to not
		## conflict with another filter's.
		path: string &optional;

		## A function returning the output path for recording entries
		## matching this filter. This is similar to *path* yet allows
		## to compute the string dynamically. It is ok to return
		## different strings for separate calls, but be careful: it's
		## easy to flood the disk by returning a new string for each
		## connection.  Upon adding a filter to a stream, if neither
		## ``path`` nor ``path_func`` is explicitly set by them, then
		## :zeek:see:`Log::default_path_func` is used.
		##
		## id: The ID associated with the log stream.
		##
		## path: A suggested path value, which may be either the filter's
		##       ``path`` if defined, else a previous result from the
		##       function.  If no ``path`` is defined for the filter,
		##       then the first call to the function will contain an
		##       empty string.
		##
		## rec: An instance of the stream's ``columns`` type with its
		##      fields set to the values to be logged.
		##
		## Returns: The path to be used for the filter, which will be
		##          subject to the same automatic correction rules as
		##          the *path* field of :zeek:type:`Log::Filter` in the
		##          case of conflicts with other filters trying to use
		##          the same writer/path pair.
		path_func: function(id: ID, path: string, rec: any): string &optional;

		## Subset of column names to record. If not given, all
		## columns are recorded.
		include: set[string] &optional;

		## Subset of column names to exclude from recording. If not
		## given, all columns are recorded.
		exclude: set[string] &optional;

		## If true, entries are recorded locally.
		log_local: bool &default=enable_local_logging;

		## If true, entries are passed on to remote peers.
		log_remote: bool &default=enable_remote_logging;

		## Field name map to rename fields before the fields are written
		## to the output.
		field_name_map: table[string] of string &default=default_field_name_map;

		## A string that is used for unrolling and flattening field names
		## for nested record types.
		scope_sep: string &default=default_scope_sep;

		## Default prefix for all extension fields. It's typically
		## prudent to set this to something that Zeek's logging
		## framework can't normally write out in a field name.
		ext_prefix: string &default=default_ext_prefix;

		## Function to collect a log extension value.  If not specified,
		## no log extension will be provided for the log.
		## The return value from the function *must* be a record.
		ext_func: function(path: string): any &default=default_ext_func;

		## Rotation interval. Zero disables rotation.
		interv: interval &default=default_rotation_interval;

		## Callback function to trigger for rotated files. If not set, the
		## default comes out of :zeek:id:`Log::default_rotation_postprocessors`.
		postprocessor: function(info: RotationInfo) : bool &optional;

		## A key/value table that will be passed on to the writer.
		## Interpretation of the values is left to the writer, but
		## usually they will be used for configuration purposes.
		config: table[string] of string &default=table();
	};

	## Sentinel value for indicating that a filter was not found when looked up.
	const no_filter: Filter = [$name="<not found>"];

	## Creates a new logging stream with the default filter.
	##
	## id: The ID enum to be associated with the new logging stream.
	##
	## stream: A record defining the content that the new stream will log.
	##
	## Returns: True if a new logging stream was successfully created and
	##          a default filter added to it.
	##
	## .. zeek:see:: Log::add_default_filter Log::remove_default_filter
	global create_stream: function(id: ID, stream: Stream) : bool;

	## Removes a logging stream completely, stopping all the threads.
	##
	## id: The ID associated with the logging stream.
	##
	## Returns: True if the stream was successfully removed.
	##
	## .. zeek:see:: Log::create_stream
	global remove_stream: function(id: ID) : bool;

	## Enables a previously disabled logging stream.  Disabled streams
	## will not be written to until they are enabled again.  New streams
	## are enabled by default.
	##
	## id: The ID associated with the logging stream to enable.
	##
	## Returns: True if the stream is re-enabled or was not previously disabled.
	##
	## .. zeek:see:: Log::disable_stream
	global enable_stream: function(id: ID) : bool;

	## Disables a currently enabled logging stream.  Disabled streams
	## will not be written to until they are enabled again.  New streams
	## are enabled by default.
	##
	## id: The ID associated with the logging stream to disable.
	##
	## Returns: True if the stream is now disabled or was already disabled.
	##
	## .. zeek:see:: Log::enable_stream
	global disable_stream: function(id: ID) : bool;

	## Adds a custom filter to an existing logging stream.  If a filter
	## with a matching ``name`` field already exists for the stream, it
	## is removed when the new filter is successfully added.
	##
	## id: The ID associated with the logging stream to filter.
	##
	## filter: A record describing the desired logging parameters.
	##
	## Returns: True if the filter was successfully added, false if
	##          the filter was not added or the *filter* argument was not
	##          the correct type.
	##
	## .. zeek:see:: Log::remove_filter Log::add_default_filter
	##    Log::remove_default_filter Log::get_filter Log::get_filter_names
	global add_filter: function(id: ID, filter: Filter) : bool;

	## Removes a filter from an existing logging stream.
	##
	## id: The ID associated with the logging stream from which to
	##     remove a filter.
	##
	## name: A string to match against the ``name`` field of a
	##       :zeek:type:`Log::Filter` for identification purposes.
	##
	## Returns: True if the logging stream's filter was removed or
	##          if no filter associated with *name* was found.
	##
	## .. zeek:see:: Log::remove_filter Log::add_default_filter
	##    Log::remove_default_filter Log::get_filter Log::get_filter_names
	global remove_filter: function(id: ID, name: string) : bool;

	## Gets the names of all filters associated with an existing
	## logging stream.
	##
	## id: The ID of a logging stream from which to obtain the list
	##     of filter names.
	##
	## Returns: The set of filter names associated with the stream.
	##
	## ..zeek:see:: Log::remove_filter Log::add_default_filter
	##   Log::remove_default_filter Log::get_filter
	global get_filter_names: function(id: ID) : set[string];

	## Gets a filter associated with an existing logging stream.
	##
	## id: The ID associated with a logging stream from which to
	##     obtain one of its filters.
	##
	## name: A string to match against the ``name`` field of a
	##       :zeek:type:`Log::Filter` for identification purposes.
	##
	## Returns: A filter attached to the logging stream *id* matching
	##          *name* or, if no matches are found returns the
	##          :zeek:id:`Log::no_filter` sentinel value.
	##
	## .. zeek:see:: Log::add_filter Log::remove_filter Log::add_default_filter
	##              Log::remove_default_filter Log::get_filter_names
	global get_filter: function(id: ID, name: string) : Filter;

	## Writes a new log line/entry to a logging stream.
	##
	## id: The ID associated with a logging stream to be written to.
	##
	## columns: A record value describing the values of each field/column
	##          to write to the log stream.
	##
	## Returns: True if the stream was found and no error occurred in writing
	##          to it or if the stream was disabled and nothing was written.
	##          False if the stream was not found, or the *columns*
	##          argument did not match what the stream was initially defined
	##          to handle, or one of the stream's filters has an invalid
	##          ``path_func``.
	##
	## .. zeek:see:: Log::enable_stream Log::disable_stream
	global write: function(id: ID, columns: any) : bool;

	## Sets the buffering status for all the writers of a given logging stream.
	## A given writer implementation may or may not support buffering and if
	## it doesn't then toggling buffering with this function has no effect.
	##
	## id: The ID associated with a logging stream for which to
	##     enable/disable buffering.
	##
	## buffered: Whether to enable or disable log buffering.
	##
	## Returns: True if buffering status was set, false if the logging stream
	##          does not exist.
	##
	## .. zeek:see:: Log::flush
	global set_buf: function(id: ID, buffered: bool): bool;

	## Flushes any currently buffered output for all the writers of a given
	## logging stream.
	##
	## id: The ID associated with a logging stream for which to flush buffered
	##     data.
	##
	## Returns: True if all writers of a log stream were signalled to flush
	##          buffered data or if the logging stream is disabled,
	##          false if the logging stream does not exist.
	##
	## .. zeek:see:: Log::set_buf Log::enable_stream Log::disable_stream
	global flush: function(id: ID): bool;

	## Adds a default :zeek:type:`Log::Filter` record with ``name`` field
	## set as "default" to a given logging stream.
	##
	## id: The ID associated with a logging stream for which to add a default
	##     filter.
	##
	## Returns: The status of a call to :zeek:id:`Log::add_filter` using a
	##          default :zeek:type:`Log::Filter` argument with ``name`` field
	##          set to "default".
	##
	## .. zeek:see:: Log::add_filter Log::remove_filter
	##    Log::remove_default_filter
	global add_default_filter: function(id: ID) : bool;

	## Removes the :zeek:type:`Log::Filter` with ``name`` field equal to
	## "default".
	##
	## id: The ID associated with a logging stream from which to remove the
	##     default filter.
	##
	## Returns: The status of a call to :zeek:id:`Log::remove_filter` using
	##          "default" as the argument.
	##
	## .. zeek:see:: Log::add_filter Log::remove_filter Log::add_default_filter
	global remove_default_filter: function(id: ID) : bool;

	## Runs a command given by :zeek:id:`Log::default_rotation_postprocessor_cmd`
	## on a rotated file.  Meant to be called from postprocessor functions
	## that are added to :zeek:id:`Log::default_rotation_postprocessors`.
	##
	## info: A record holding meta-information about the log being rotated.
	##
	## npath: The new path of the file (after already being rotated/processed
	##        by writer-specific postprocessor as defined in
	##        :zeek:id:`Log::default_rotation_postprocessors`).
	##
	## Returns: True when :zeek:id:`Log::default_rotation_postprocessor_cmd`
	##          is empty or the system command given by it has been invoked
	##          to postprocess a rotated log file.
	##
	## .. zeek:see:: Log::default_rotation_date_format
	##    Log::default_rotation_postprocessor_cmd
	##    Log::default_rotation_postprocessors
	global run_rotation_postprocessor_cmd: function(info: RotationInfo, npath: string) : bool;

	## The streams which are currently active and not disabled.
	## This table is not meant to be modified by users!  Only use it for
	## examining which streams are active.
	global active_streams: table[ID] of Stream = table();
}

global all_streams: table[ID] of Stream = table();

global stream_filters: table[ID] of set[string] = table();

# We keep a script-level copy of all filters so that we can manipulate them.
global filters: table[ID, string] of Filter;

@load base/bif/logging.bif # Needs Filter and Stream defined.

module Log;

# Used internally by the log manager.
function __default_rotation_postprocessor(info: RotationInfo) : bool
	{
	if ( info$writer in default_rotation_postprocessors )
		return default_rotation_postprocessors[info$writer](info);
	else
		# Return T by default so that postprocessor-less writers don't shutdown.
		return T;
	}

function default_path_func(id: ID, path: string, rec: any) : string
	{
	# The suggested path value is a previous result of this function
	# or a filter path explicitly set by the user, so continue using it.
	if ( path != "" )
		return path;

	local id_str = fmt("%s", id);

	local parts = split_string1(id_str, /::/);
	if ( |parts| == 2 )
		{
		# Example: Notice::LOG -> "notice"
		if ( parts[1] == "LOG" )
			{
			local module_parts = split_string_n(parts[0], /[^A-Z][A-Z][a-z]*/, T, 4);
			local output = "";
			if ( 0 in module_parts )
				output = module_parts[0];
			if ( 1 in module_parts && module_parts[1] != "" )
				output = cat(output, sub_bytes(module_parts[1],1,1), "_", sub_bytes(module_parts[1], 2, |module_parts[1]|));
			if ( 2 in module_parts && module_parts[2] != "" )
				output = cat(output, "_", module_parts[2]);
			if ( 3 in module_parts && module_parts[3] != "" )
				output = cat(output, sub_bytes(module_parts[3],1,1), "_", sub_bytes(module_parts[3], 2, |module_parts[3]|));
			return to_lower(output);
			}

		# Example: Notice::POLICY_LOG -> "notice_policy"
		if ( /_LOG$/ in parts[1] )
			parts[1] = sub(parts[1], /_LOG$/, "");

		return cat(to_lower(parts[0]),"_",to_lower(parts[1]));
		}
	else
		return to_lower(id_str);
	}

# Run post-processor on file. If there isn't any postprocessor defined,
# we move the file to a nicer name.
function run_rotation_postprocessor_cmd(info: RotationInfo, npath: string) : bool
	{
	local pp_cmd = default_rotation_postprocessor_cmd;

	if ( pp_cmd == "" )
		return T;

	# Turn, e.g., Log::WRITER_ASCII into "ascii".
	local writer = subst_string(to_lower(fmt("%s", info$writer)), "log::writer_", "");

	# The date format is hard-coded here to provide a standardized
	# script interface.
	system(fmt("%s %s %s %s %s %d %s",
               pp_cmd, npath, info$path,
               strftime("%y-%m-%d_%H.%M.%S", info$open),
               strftime("%y-%m-%d_%H.%M.%S", info$close),
               info$terminating, writer));

	return T;
	}

function create_stream(id: ID, stream: Stream) : bool
	{
	if ( ! __create_stream(id, stream) )
		return F;

	active_streams[id] = stream;
	all_streams[id] = stream;

	return add_default_filter(id);
	}

function remove_stream(id: ID) : bool
	{
	delete active_streams[id];
	delete all_streams[id];

	if ( id in stream_filters )
		{
		for ( i in stream_filters[id] )
			delete filters[id, i];

		delete stream_filters[id];
		}
	return __remove_stream(id);
	}

function disable_stream(id: ID) : bool
	{
	delete active_streams[id];
	return __disable_stream(id);
	}

function enable_stream(id: ID) : bool
	{
	if ( ! __enable_stream(id) )
		return F;

	if ( id in all_streams )
		active_streams[id] = all_streams[id];
	}

# convenience function to add a filter name to stream_filters
function add_stream_filters(id: ID, name: string)
	{
	if ( id in stream_filters )
		add stream_filters[id][name];
	else
		stream_filters[id] = set(name);
	}

function add_filter(id: ID, filter: Filter) : bool
	{
	local stream = all_streams[id];

	if ( stream?$path && ! filter?$path )
		filter$path = stream$path;

	if ( ! filter?$path && ! filter?$path_func )
		filter$path_func = default_path_func;

	local res = __add_filter(id, filter);
	if ( res )
		{
		add_stream_filters(id, filter$name);
		filters[id, filter$name] = filter;
		}
	return res;
	}

function remove_filter(id: ID, name: string) : bool
	{
	if ( id in stream_filters )
		delete stream_filters[id][name];

	delete filters[id, name];

	return __remove_filter(id, name);
	}

function get_filter(id: ID, name: string) : Filter
	{
	if ( [id, name] in filters )
		return filters[id, name];

	return no_filter;
	}

function get_filter_names(id: ID) : set[string]
	{
	if ( id in stream_filters )
		return stream_filters[id];
	else
		return set();
	}

function write(id: ID, columns: any) : bool
	{
	return __write(id, columns);
	}

function set_buf(id: ID, buffered: bool): bool
	{
	return __set_buf(id, buffered);
	}

function flush(id: ID): bool
	{
	return __flush(id);
	}

function add_default_filter(id: ID) : bool
	{
	return add_filter(id, [$name="default"]);
	}

function remove_default_filter(id: ID) : bool
	{
	return remove_filter(id, "default");
	}
