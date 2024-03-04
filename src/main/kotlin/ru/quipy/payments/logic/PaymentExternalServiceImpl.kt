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
        .dispatcher(Dispatcher(Executors.newSingleThreadExecutor()))
        .build()

    // Cập nhật logic để chọn tài khoản dựa trên SLA hoặc tiêu chí khác
    private fun selectAccountForPayment(): ExternalServiceProperties {
        // Ví dụ: chọn accountProps2 nếu SLA cho phép, ngược lại chọn accountProps1
        // return if (accountProps2.request95thPercentileProcessingTime.toMillis() <= 80000) accountProps2 else accountProps1
        return if (lastUsedAccount == accountProps1) {
            accountProps2
        } else {
            accountProps1
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val selectedAccount = selectAccountForPayment() // Sử dụng hàm đã thêm để chọn tài khoản
        lastUsedAccount = selectedAccount
        logger.warn("[${selectedAccount.accountName}] Submitting payment request for payment $paymentId. Already passed: ${System.currentTimeMillis() - paymentStartedAt} ms")

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
