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

class PaymentExternalServiceImpl(
    private val accountProps1: ExternalServiceProperties, 
    private val accountProps2: ExternalServiceProperties  
) : PaymentExternalService {

    // private var lastUsedAccount: ExternalServiceProperties? = null

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        val paymentOperationTimeout = Duration.ofSeconds(80)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher(Executors.newSingleThreadExecutor()))
        .build()

    private fun calculateSpeed(account: ExternalServiceProperties): Double {
        val p = account.parallelRequests.toDouble()
        val r = account.rateLimitPerSec.toDouble()
        val a = account.request95thPercentileProcessingTime.toMillis().toDouble() / 1000.0 // Convert milliseconds to seconds for 'A'
        // The effective rate limited by R and A
        val effectiveR = r / a
        // The speed is defined as the minimum of P and effective R
        return minOf(p, effectiveR)
    }

    private fun estimateProcessingTime(account: ExternalServiceProperties): Long {
        val speed = calculateSpeed(account)
        // Assuming the service processes one request at a time, the time to process one request
        return (1 / speed * 1000).toLong() // Convert seconds to milliseconds
    }

    // private fun selectAccountForPayment(startTime: Long): ExternalServiceProperties? {
    //     val elapsedTime = System.currentTimeMillis() - startTime
    //     val remainingTimeForSLA = SLA_THRESHOLD - elapsedTime

    //     val timeForRequest1 = estimateProcessingTime(accountProps1)
    //     val timeForRequest2 = estimateProcessingTime(accountProps2)

    //     // Prioritize using account 2 (cheaper) if within SLA; switch to account 1 otherwise
    //     return when {
    //         elapsedTime + timeForRequest2 <= remainingTimeForSLA -> accountProps2
    //         elapsedTime + timeForRequest1 <= remainingTimeForSLA -> accountProps1
    //         else -> null // Do not send the request if neither account can meet the SLA
    //     }
    // }

    private fun selectAccountForPayment(startTime: Long): ExternalServiceProperties {
        val timeElapsed = System.currentTimeMillis() - startTime
        val timeLeftForSLA = paymentOperationTimeout.seconds * 1000 - timeElapsed

        val speed1 = estimateProcessingTime(accountProps1)
        val speed2 = estimateProcessingTime(accountProps2)

        val slaMetAccount2 = accountProps2.request95thPercentileProcessingTime.toMillis() <= timeLeftForSLA
        val slaMetAccount1 = accountProps1.request95thPercentileProcessingTime.toMillis() <= timeLeftForSLA

        return when {
            slaMetAccount2 -> accountProps2
            slaMetAccount1 -> accountProps1
            else -> if (speed1 > speed2) accountProps1 else accountProps2
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val selectedAccount = selectAccountForPayment(paymentStartedAt)
        // if (selectedAccount == null) {
        //     logger.error("Unable to process payment $paymentId. Neither account can meet the SLA.")
        //     return
        // }

        // lastUsedAccount = selectedAccount
        logger.info("[${selectedAccount.accountName}] Selected for payment $paymentId, elapsed time: ${System.currentTimeMillis() - paymentStartedAt} ms")
        
        val transactionId = UUID.randomUUID()
        logger.info("[${selectedAccount.accountName}] Submit for $paymentId, txId: $transactionId")

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${selectedAccount.serviceName}&accountName=${selectedAccount.accountName}&transactionId=$transactionId")
            post(emptyBody)
            build()
        }
        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${selectedAccount.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${selectedAccount.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[${selectedAccount.accountName}] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }
}

public fun now() = System.currentTimeMillis()
