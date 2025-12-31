/**
 * @file zpdf.h
 * @brief C header for zpdf library FFI
 *
 * This header declares the C API exported by zpdf (https://github.com/Lulzx/zpdf).
 * zpdf is a high-performance PDF text extraction library written in Zig.
 */

#ifndef ZPDF_H
#define ZPDF_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Opaque document handle.
 * Created by zpdf_open() or zpdf_open_memory(), freed by zpdf_close().
 */
typedef struct ZpdfDocument ZpdfDocument;

/**
 * Text span with bounding box information.
 * Used by zpdf_extract_bounds() for layout-aware extraction.
 */
typedef struct {
    double x0, y0, x1, y1; /**< Bounding box coordinates */
    const uint8_t* text;   /**< Text content (not null-terminated) */
    size_t text_len;       /**< Length of text in bytes */
    double font_size;      /**< Font size in points */
} CTextSpan;

/* ============================================================================
 * Document Lifecycle
 * ============================================================================ */

/**
 * Open a PDF document from a file path.
 *
 * @param path Null-terminated file path
 * @return Document handle, or NULL on error
 */
ZpdfDocument* zpdf_open(const char* path);

/**
 * Open a PDF document from a memory buffer.
 *
 * @param data Pointer to PDF data
 * @param len Length of data in bytes
 * @return Document handle, or NULL on error
 */
ZpdfDocument* zpdf_open_memory(const uint8_t* data, size_t len);

/**
 * Close a document and free associated resources.
 *
 * @param handle Document handle (may be NULL)
 */
void zpdf_close(ZpdfDocument* handle);

/* ============================================================================
 * Page Information
 * ============================================================================ */

/**
 * Get the number of pages in the document.
 *
 * @param handle Document handle
 * @return Page count (>= 0), or -1 on error
 */
int zpdf_page_count(ZpdfDocument* handle);

/**
 * Get information about a specific page.
 *
 * @param handle Document handle
 * @param page_num Page number (0-indexed)
 * @param width Output: page width in points
 * @param height Output: page height in points
 * @param rotation Output: page rotation in degrees (0, 90, 180, 270)
 * @return 0 on success, -1 on error
 */
int zpdf_get_page_info(ZpdfDocument* handle, int page_num, double* width, double* height,
                       int* rotation);

/* ============================================================================
 * Text Extraction
 * ============================================================================ */

/**
 * Extract text from a single page.
 *
 * @param handle Document handle
 * @param page_num Page number (0-indexed)
 * @param out_len Output: length of extracted text
 * @return Extracted text (caller must free with zpdf_free_buffer), or NULL on error
 */
uint8_t* zpdf_extract_page(ZpdfDocument* handle, int page_num, size_t* out_len);

/**
 * Extract text from all pages (sequential processing).
 *
 * @param handle Document handle
 * @param out_len Output: length of extracted text
 * @return Extracted text (caller must free with zpdf_free_buffer), or NULL on error
 */
uint8_t* zpdf_extract_all(ZpdfDocument* handle, size_t* out_len);

/**
 * Extract text from all pages using parallel processing.
 * This is the fastest extraction method for multi-page documents.
 *
 * @param handle Document handle
 * @param out_len Output: length of extracted text
 * @return Extracted text (caller must free with zpdf_free_buffer), or NULL on error
 */
uint8_t* zpdf_extract_all_parallel(ZpdfDocument* handle, size_t* out_len);

/* ============================================================================
 * Reading Order Extraction (Experimental)
 * ============================================================================ */

/**
 * Extract text from a single page in visual reading order.
 * This attempts to order text as it would be read visually.
 *
 * @param handle Document handle
 * @param page_num Page number (0-indexed)
 * @param out_len Output: length of extracted text
 * @return Extracted text (caller must free with zpdf_free_buffer), or NULL on error
 */
uint8_t* zpdf_extract_page_reading_order(ZpdfDocument* handle, int page_num, size_t* out_len);

/**
 * Extract text from all pages in visual reading order (sequential).
 *
 * @param handle Document handle
 * @param out_len Output: length of extracted text
 * @return Extracted text (caller must free with zpdf_free_buffer), or NULL on error
 */
uint8_t* zpdf_extract_all_reading_order(ZpdfDocument* handle, size_t* out_len);

/**
 * Extract text from all pages in visual reading order (parallel).
 *
 * @param handle Document handle
 * @param out_len Output: length of extracted text
 * @return Extracted text (caller must free with zpdf_free_buffer), or NULL on error
 */
uint8_t* zpdf_extract_all_reading_order_parallel(ZpdfDocument* handle, size_t* out_len);

/* ============================================================================
 * Bounds Extraction
 * ============================================================================ */

/**
 * Extract text spans with bounding box information.
 *
 * @param handle Document handle
 * @param page_num Page number (0-indexed)
 * @param out_count Output: number of spans
 * @return Array of text spans (caller must free with zpdf_free_bounds), or NULL on error
 */
CTextSpan* zpdf_extract_bounds(ZpdfDocument* handle, int page_num, size_t* out_count);

/* ============================================================================
 * Memory Management
 * ============================================================================ */

/**
 * Free a text buffer returned by extraction functions.
 *
 * @param ptr Buffer pointer (may be NULL)
 * @param len Buffer length
 */
void zpdf_free_buffer(uint8_t* ptr, size_t len);

/**
 * Free a text spans array returned by zpdf_extract_bounds().
 *
 * @param ptr Spans array pointer (may be NULL)
 * @param count Number of spans
 */
void zpdf_free_bounds(CTextSpan* ptr, size_t count);

#ifdef __cplusplus
}
#endif

#endif /* ZPDF_H */
