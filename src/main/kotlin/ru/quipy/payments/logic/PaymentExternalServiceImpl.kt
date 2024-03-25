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
import java.io.IOException


class PaymentExternalServiceImpl(
    private val accountProps: ExternalServiceProperties, 
    private val fallbackService: PaymentExternalServiceImpl? 
) : PaymentExternalService {

    private var lastUsedAccount: ExternalServiceProperties? = null

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        val paymentOperationTimeout = Duration.ofSeconds(80)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher(Executors.newCachedThreadPool()))
        .build()

    private fun selectAccountForPayment(): ExternalServiceProperties {

        return accountProps
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val currentTime = now()
        val selectedAccount = selectAccountForPayment() // Use the added function to select an account
        val elapsedTime = currentTime - paymentStartedAt
        if (elapsedTime + selectedAccount.request95thPercentileProcessingTime.toMillis() > paymentOperationTimeout.toMillis()) {
            logger.warn("[${selectedAccount.accountName}] Payment request for $paymentId will not be processed within operation timeout.")
            return
        }

        lastUsedAccount = selectedAccount
        logger.warn("[${selectedAccount.accountName}] Submitting payment request for payment $paymentId. Already passed: $elapsedTime ms")

        val transactionId = UUID.randomUUID()
        logger.info("[${selectedAccount.accountName}] Submit for $paymentId, txId: $transactionId")

        // Mark the payment as submitted before sending the request
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, currentTime, Duration.ofMillis(elapsedTime))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${selectedAccount.serviceName}&accountName=${selectedAccount.accountName}&transactionId=$transactionId")
            post(emptyBody)
            build()
        }

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${selectedAccount.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${selectedAccount.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                logger.error("[${selectedAccount.accountName}] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = e.message ?: "Unknown error")
                }
            }
        })
    }
}

public fun now() = System.currentTimeMillis()
