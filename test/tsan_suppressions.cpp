/*
 * SPMC consumers read frames the producer may be overwriting and discard
 * them afterwards (seqlock style), so ThreadSanitizer reports those reads
 * by design. The frames are inlined and symbolize without their class, so
 * the suppression matches on the header that holds both sides instead.
 */
extern "C" const char *__tsan_default_suppressions()
{
	return "race:multiqueues/spmc.hpp\n";
}
