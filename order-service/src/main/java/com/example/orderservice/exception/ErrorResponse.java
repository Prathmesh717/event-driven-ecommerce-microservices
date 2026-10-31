package io.github.prathmesh717.orderservice.exception;

import java.time.LocalDateTime;

/**
 * Standard error response returned by APIs.
 */
public record ErrorResponse(

        LocalDateTime timestamp,

        int status,

        String error,

        String message,

        String path

) {
}