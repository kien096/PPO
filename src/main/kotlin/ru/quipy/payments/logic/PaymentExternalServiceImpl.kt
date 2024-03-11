package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min
import java.util.concurrent.atomic.AtomicLong


class PaymentExternalServiceImpl(
    private val accountProps1: ExternalServiceProperties,
    private val accountProps2: ExternalServiceProperties
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        val paymentOperationTimeout = Duration.ofSeconds(80).toMillis()
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val client = OkHttpClient.Builder().dispatcher(Dispatcher(Executors.newSingleThreadExecutor())).build()

    // Atomic counters for window control
    private val activeRequestsAccount1 = AtomicInteger(0)
    private val activeRequestsAccount2 = AtomicInteger(0)

    // Rate limit tracking
    private val lastRequestTimeAccount1 = AtomicLong(0)
    private val lastRequestTimeAccount2 = AtomicLong(0)

    private fun canMakeRequest(account: ExternalServiceProperties, lastRequestTime: AtomicLong): Boolean {
        val currentTime = System.currentTimeMillis()
        val rateLimitInterval = 1000L / account.rateLimitPerSec // ms per request
        return currentTime - lastRequestTime.get() >= rateLimitInterval
    }

    private fun selectAccountForPayment(startTime: Long): ExternalServiceProperties {
        val timeElapsed = System.currentTimeMillis() - startTime
        val timeLeftForSLA = paymentOperationTimeout - timeElapsed

        // Prioritize using account 2 if within SLA and respects window control and rate limit; switch to account 1 otherwise.
        val useAccount2 = activeRequestsAccount2.get() < accountProps2.parallelRequests &&
                          canMakeRequest(accountProps2, lastRequestTimeAccount2) &&
                          accountProps2.request95thPercentileProcessingTime.toMillis() <= timeLeftForSLA

        val useAccount1 = activeRequestsAccount1.get() < accountProps1.parallelRequests &&
                          canMakeRequest(accountProps1, lastRequestTimeAccount1) &&
                          accountProps1.request95thPercentileProcessingTime.toMillis() <= timeLeftForSLA

        return when {
            useAccount2 -> {
                lastRequestTimeAccount2.set(System.currentTimeMillis())
                activeRequestsAccount2.incrementAndGet()
                accountProps2
            }
            useAccount1 -> {
                lastRequestTimeAccount1.set(System.currentTimeMillis())
                activeRequestsAccount1.incrementAndGet()
                accountProps1
            }
            else -> {
                // Fallback logic if neither account is viable at this moment
                // This may be to retry after a delay or to reject the request upfront
                if (accountProps2.request95thPercentileProcessingTime.toMillis() < accountProps1.request95thPercentileProcessingTime.toMillis()) {
                    accountProps2
                } else {
                    accountProps1
                }
            }
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val selectedAccount = selectAccountForPayment(paymentStartedAt)

        logger.info("[${selectedAccount.accountName}] Selected for payment $paymentId, elapsed time: ${System.currentTimeMillis() - paymentStartedAt} ms")
        
        val transactionId = UUID.randomUUID()
        logger.info("[${selectedAccount.accountName}] Submit for $paymentId, txId: $transactionId")

        val request = Request.Builder().url("http://localhost:1234/external/process?serviceName=${selectedAccount.serviceName}&accountName=${selectedAccount.accountName}&transactionId=$transactionId").post(emptyBody).build()
        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${selectedAccount.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${selectedAccount.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, System.currentTimeMillis(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, System.currentTimeMillis(), transactionId, reason = "Request timeout.")
                    }
                }
                else -> {
                    logger.error("[${selectedAccount.accountName}] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, System.currentTimeMillis(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            if (selectedAccount == accountProps1) {
                activeRequestsAccount1.decrementAndGet()
            } else {
                activeRequestsAccount2.decrementAndGet()
            }
        }
    }
}

fun now() = System.currentTimeMillis()
