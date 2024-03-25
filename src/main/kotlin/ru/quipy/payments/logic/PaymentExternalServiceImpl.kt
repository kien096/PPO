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
import java.util.concurrent.*
import java.io.IOException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


class PaymentExternalServiceImpl(
    private val accountProps4: ExternalServiceProperties,
    private val accountProps3: ExternalServiceProperties,
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

    // Initialize OkHttpClient with HTTP/2 support and no maximum requests limit
    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher(Executors.newCachedThreadPool()).apply {
            maxRequests = Integer.MAX_VALUE // Ensuring no default limit on concurrent requests
            maxRequestsPerHost = Integer.MAX_VALUE
        })
        .protocols(listOf(Protocol.HTTP_2, Protocol.HTTP_1_1)) 
        .build()

    // Executors for response handling
    private val responseHandlerExecutor: ExecutorService = Executors.newCachedThreadPool()

    
    private val activeRequestsAccount2 = AtomicInteger(0)
    private val activeRequestsAccount3 = AtomicInteger(0)
    private val activeRequestsAccount4 = AtomicInteger(0)

    private val lastRequestTimeAccount2 = AtomicLong(0)
    private val lastRequestTimeAccount3 = AtomicLong(0)
    private val lastRequestTimeAccount4 = AtomicLong(0)

    // Queues for each account
    private val queues = mapOf(
        accountProps4 to ConcurrentLinkedQueue<Runnable>(),
        accountProps3 to ConcurrentLinkedQueue<Runnable>(),
        accountProps2 to ConcurrentLinkedQueue<Runnable>()
    )

    // Queue processing
    private val queueProcessor = Executors.newScheduledThreadPool(3) 

    init {
        queues.forEach { (account, queue) ->
            queueProcessor.scheduleWithFixedDelay({
                while (queue.isNotEmpty()) {
                    val task = queue.poll()
                    responseHandlerExecutor.execute(task)
                }
            }, 0, 1, TimeUnit.MILLISECONDS) // Immediate execution with a delay for polling the queue
        }
    }

    
    // private fun canMakeRequest(account: ExternalServiceProperties): Boolean {
    //     val activeRequestCount = activeRequests[account]?.get() ?: return false
    //     val lastRequestTime = lastRequestTimeAccount.getOrDefault(account, AtomicLong(0))
    //     val currentTime = System.currentTimeMillis()
    //     val rateLimitInterval = 1000L / account.rateLimitPerSec
    //     return (currentTime - lastRequestTime.get() >= rateLimitInterval) && (activeRequestCount < account.parallelRequests)
    // }
    private fun canMakeRequest(account: ExternalServiceProperties, lastRequestTime: AtomicLong): Boolean {
        val currentTime = System.currentTimeMillis()
        val rateLimitInterval = 1000L / account.rateLimitPerSec
        return currentTime - lastRequestTime.get() >= rateLimitInterval &&
               (activeRequestsAccount2.get() < account.parallelRequests ||
               activeRequestsAccount3.get() < account.parallelRequests ||
               activeRequestsAccount4.get() < account.parallelRequests)
    }

    // Logic to select the best account for payment based on SLA and rate limiting
    // private fun selectAccountForPayment(startTime: Long): ExternalServiceProperties {
    //     val elapsedTime = System.currentTimeMillis() - startTime
    //     val timeLeftForSLA = paymentOperationTimeout - elapsedTime

    //     val accountsInOrderOfPriority = listOf(accountProps4, accountProps3, accountProps2)

    //     return accountsInOrderOfPriority.firstOrNull { account ->
    //         canMakeRequest(account) && account.request95thPercentileProcessingTime.toMillis() <= timeLeftForSLA
    //     } ?: throw IllegalStateException("No available accounts meet the SLA requirements.")
    // }

    private fun selectAccountForPayment(startTime: Long): ExternalServiceProperties {
        val timeElapsed = System.currentTimeMillis() - startTime
        val timeLeftForSLA = paymentOperationTimeout - timeElapsed

        
        val sortedAccountsByPriority = listOf(accountProps4, accountProps3, accountProps2)
            .sortedBy { it.request95thPercentileProcessingTime.toMillis() }

        for (account in sortedAccountsByPriority) {
            val lastRequestTime = when (account) {
                accountProps2 -> lastRequestTimeAccount2
                accountProps3 -> lastRequestTimeAccount3
                accountProps4 -> lastRequestTimeAccount4
                else -> null
            }

            val queueSize = queues[account]?.size ?: Int.MAX_VALUE
            if (queueSize < account.parallelRequests &&
                canMakeRequest(account, lastRequestTime!!) &&
                account.request95thPercentileProcessingTime.toMillis() <= timeLeftForSLA) {
                
                lastRequestTime.set(System.currentTimeMillis())
                return account
            }
        }

        throw IllegalStateException("No available accounts meet the SLA requirements or rate limits.")
    }


    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val startTime = System.currentTimeMillis()
        // Select the account based on current logic; adjust as necessary.
        val selectedAccount = selectAccountForPayment(startTime)

        logger.warn("[${selectedAccount.accountName}] Submitting payment request for payment $paymentId. Already passed: ${now() - startTime} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[${selectedAccount.accountName}] Submit for $paymentId, txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val requestUrl = "http://localhost:1234/external/process?serviceName=${selectedAccount.serviceName}&accountName=${selectedAccount.accountName}&transactionId=$transactionId&amount=$amount"
        val request = Request.Builder().url(requestUrl).post(emptyBody).build()

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                response.use { res ->
                    val body = try {
                        mapper.readValue(res.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[${selectedAccount.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${res.code}, reason: ${res.body?.string()}")
                        ExternalSysResponse(false, e.message)
                    }

                    logger.warn("[${selectedAccount.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                logger.error("[${selectedAccount.accountName}] Payment failed for txId: $transactionId, payment: $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, "Request failed: ${e.message}")
                }
            }
        })
    }

}

fun now() = System.currentTimeMillis()
