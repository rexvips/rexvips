package com.paytm.bank.product.externalservice;

import com.commons.dto.request.CardLimitUpdateRequest;
import com.commons.dto.request.ChannelAccessUpdateRequest;
import com.commons.dto.request.CorpCardCifCreationRequest;
import com.commons.dto.request.CorpCardCustomerOnBoardingRequest;
import com.commons.dto.request.IssueCardRequest;
import com.commons.dto.request.LimitAndChannelUpdateRequest;
import com.commons.dto.request.ProofDetail;
import com.commons.dto.request.ReIssuanceCorpCardRequest;
import com.commons.dto.request.ReIssuanceRequest;
import com.commons.dto.request.instacard.CardOrderRequest;
import com.commons.dto.request.instacard.InstaAddress;
import com.commons.dto.request.instacard.InstaCardReplacement;
import com.commons.dto.request.instacard.InstaIssueRequest;
import com.commons.dto.request.instacard.QrMappingRequest;
import com.commons.dto.request.physicaldebitcard.AddressDetails;
import com.commons.dto.request.physicaldebitcard.IssuePDCRequest;
import com.commons.dto.request.physicaldebitcard.UpdateCustomerRequest;
import com.commons.dto.response.BaseResponse;
import com.commons.dto.response.CardDetailsResponse;
import com.commons.dto.response.CardDetailsV3Response;
import com.commons.dto.response.CustomResponse;
import com.commons.dto.response.CustomerDetails;
import com.commons.dto.response.DebitCardDetailsResponse;
import com.commons.dto.response.EncryptedCardDetailsResponse;
import com.commons.dto.response.FailureResponse;
import com.commons.dto.response.IssueCardResponse;
import com.commons.dto.response.KycResponse;
import com.commons.dto.response.UserDetailsResponse;
import com.commons.dto.response.instacard.CardOrderResponse;
import com.commons.dto.response.instacard.CardOrderStatusResponse;
import com.commons.dto.response.instacard.InstaCardReplacementResponse;
import com.commons.dto.response.instacard.QrMappingResponse;
import com.commons.dto.response.physicaldebitcard.IssDirectiveResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.hystrix.exception.HystrixRuntimeException;
import com.netflix.hystrix.exception.HystrixTimeoutException;
import com.paytm.bank.product.constants.Constants;
import com.paytm.bank.product.constants.Constants.FIS_PROPERTY;
import com.paytm.bank.product.constants.FISParameterLengthLimit;
import com.paytm.bank.product.constants.MessageConstants;
import com.paytm.bank.product.constants.StateMachineConstants;
import com.paytm.bank.product.dto.api.ApiResponse;
import com.paytm.bank.product.enums.CaPdcCardCodes;
import com.paytm.bank.product.enums.DebitCardStatusEnum;
import com.paytm.bank.product.enums.DependentServiceEnum;
import com.paytm.bank.product.exception.BadRequestException;
import com.paytm.bank.product.exception.DownStreamServerException;
import com.paytm.bank.product.exception.InternalServerException;
import com.paytm.bank.product.exception.StateMachineException;
import com.paytm.bank.product.helpers.EisHelper;
import com.paytm.bank.product.hystrix.FisCommandWrapper;
import com.paytm.bank.product.model.hibernate.DebitCardVariant;
import com.paytm.bank.product.model.hibernate.barcode.BarcodeDetails;
import com.paytm.bank.product.model.request.PDCPinSetRequest;
import com.paytm.bank.product.nach.DateUtil;
import com.paytm.bank.product.pojo.ChangeDebitCardStatus;
import com.paytm.bank.product.pojo.CustomPDCResponse;
import com.paytm.bank.product.pojo.DeliveryAddress;
import com.paytm.bank.product.pojo.IdcInvoiceRequest;
import com.paytm.bank.product.pojo.PdcCustomization;
import com.paytm.bank.product.pojo.ProductInitiationRequest;
import com.paytm.bank.product.pojo.kyb.KybAddressDetails;
import com.paytm.bank.product.pojo.kyb.KybCompanyDetails;
import com.paytm.bank.product.pojo.kyb.KybResponse;
import com.paytm.bank.product.util.AppUtils;
import com.paytm.bank.product.util.DynamicPropertyReader;
import com.paytm.bank.product.util.PSStatUtils;
import com.paytm.bank.product.util.PropertiesContext;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.paytm.bank.product.util.rest.AbstractRestClient;
import com.paytm.bank.product.util.rest.RestClientFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.ResourceAccessException;

import static com.paytm.bank.product.constants.Constants.Hystrix.FIS_ENABLE_HYSTRIX;
import static com.paytm.bank.product.constants.Constants.RequestType.GET;
import static com.paytm.bank.product.constants.Constants.RequestType.POST;
import static com.paytm.bank.product.constants.Constants.RequestType.PUT;
import static com.paytm.bank.product.constants.FISParameterLengthLimit.MAX_COURIER_NAME_LENGTH;

import javax.annotation.PostConstruct;
import org.springframework.web.util.UriComponentsBuilder;


@Service
public class FISProxy {

  final private static Logger LOGGER = LoggerFactory.getLogger(FISProxy.class);
  
  private static final String ERROR_WHILE_SETTING_DEBIT_CARD_PIN = "error while Setting debit card PIN";

  private static final String EC003 ="EC003";

  private static final String KYC_DATE_FORMAT = "dd/MM/yyyy";

  private static final String CIF_DATE_FORMAT = "yyyy-MM-dd";

  private ObjectMapper objectMapper = AppUtils.getObjectMapper();

  private AbstractRestClient restClientUtil;

  @Autowired
  private EisHelper eisHelper;

  @Autowired
  private RestClientFactory restClientFactory;

  @Value("${host.eis.domain}")
  String hostUrl;

  @Autowired
  private OauthProxy oauthProxy;

  @Value("${txn.id.header.name}")
  String requestToken;

  public String createDebitCard() {
    return "123XXX456";
  }

  final String CARD_ENDPOINT = "v2/cards";
  final String CARD_ENDPOINT2 = "v2/customers";
  final String ISSUE_PDC = "v2/physicalcards";
  final String PERM_BLOCK_CARD = "v2/cards";
  final String SET_PIN="/pin";
  final String CHECK_CARD_PIN="/pin/validate";
  final String CHANGE_CARD_STATUS = "v2/cards";
  final String ISSUE_IDC = "v2/instacards";
  final String CORPORATE_MASTER_CARD_ENDPOINT = "v2/cards/corporate/cif";
  final String CORPORATE_CARD_FOR_CA = "v2/cards/corporate/MASTER_CARD_NO";
  final String CORPORATE_CARD_RE_ISSUE_VERSION = "/v2";
  final String CORPORATE_CARD_RE_ISSUE_API = "/physicalcards/corporate/reissuance";
  final String CARD_LIMIT_AND_CHANNEL_ACCESS_UPDATE="v2/cardsv3/limitandchannelaccess";
  final String V3_CARD_DETAILS="v2/cardsv3/{alias}";
  final String CARD_LIMIT_UPDATE ="v2/cardsv3/limit";
  final String CARD_CHANNEL_ACCESS_UPDATE ="v2/cardsv3/channelaccess";
  final String ENCRYPTED_CARD_DETAILS_ENDPOINT = "/v2/cards/ext/info";
  final String INTL_USAGE_ENDPOINT ="internationalTransaction";

  @Autowired
  private FisCommandWrapper fisCommandWrapper;

  @PostConstruct
  public void init() {
    restClientUtil = restClientFactory.getRestClientUtil(DependentServiceEnum.FIS);
  }

  public IssueCardResponse createDebitCard(Map<String, Object> valueMap) {
    validateFISApiSwitch(POST);
    if(valueMap!=null && valueMap.containsKey(StateMachineConstants.CUST_ID)){
      LOGGER.debug("Create debit card request for "+valueMap.get(StateMachineConstants.CUST_ID));
    }
    String processId = MDC.get(Constants.REQUEST_ID);
    IssueCardResponse issueCardResponse = null;
    try {
      IssueCardRequest issueCardRequest = createDCRequest(valueMap);
//      oauthProxy.isCustIdAllowed(issueCardRequest.getCustomerId());
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(CARD_ENDPOINT).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, processId);
      HttpEntity<IssueCardRequest> httpEntity = new HttpEntity<>(issueCardRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} ", CARD_ENDPOINT);
      long start = System.currentTimeMillis();
//      ResponseEntity<JsonNode> responseEntity =
//          restClientUtil.makePostRequest(url, httpEntity, JsonNode.class);

      ResponseEntity<JsonNode> responseEntity = triggerRequest(url, httpEntity, JsonNode.class, POST);
      long end = System.currentTimeMillis();

      if (responseEntity != null) {
        JsonNode jsonResponse = responseEntity.getBody();
        if (jsonResponse != null) {
          JsonNode responseStatus = jsonResponse.get("status");
          if (null != responseStatus
              && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(responseStatus.asText())) {
            LOGGER.info("Got success response while creating Debit Card [Cust-Id: {}]",
                valueMap.get(StateMachineConstants.CUST_ID));

            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD, end - start);


            JsonNode jsonSuccessResponse = jsonResponse.get("response");
            if (null != jsonSuccessResponse) {
              issueCardResponse =
                  AppUtils.fromJackson(jsonSuccessResponse.toString(), IssueCardResponse.class);
            }
          }else if (null != responseStatus
              && CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
            JsonNode jsonNode = jsonResponse.get("response");
            FailureResponse failureResponse = AppUtils.getErrorFromEis(jsonNode);
            setAllMDCErrors(failureResponse, "VDC creation failed");
            PSStatUtils.incrementStats(PSStatUtils.Entity.CBS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
          } else {
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
          }
        } else {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
        }
      } else {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      }
    } catch (HystrixRuntimeException hre) {

      if (hre.getCause() instanceof HttpStatusCodeException) {
        HttpStatusCodeException hsce = (HttpStatusCodeException) hre.getCause();
        String response = hsce.getResponseBodyAsString();
        LOGGER.error("error while creating debit card  : {}", response);
        FailureResponse failureResponse =
            new FailureResponse(response, String.valueOf(hsce.getRawStatusCode()), null);
        setAllMDCErrors(failureResponse, "VDC creation failed");
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      } else if (hre.getCause() instanceof HystrixTimeoutException) {
        HystrixTimeoutException hte = (HystrixTimeoutException) hre.getCause();
        LOGGER.error("error while creating debit card  : {}", hte);
        FailureResponse failureResponse = new FailureResponse(hte.getMessage(), "504", null);
        setAllMDCErrors(failureResponse, "VDC creation failed");
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      } else {
        LOGGER.error("error while creating debitcard  : {}", hre.getMessage());
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      }
    }catch (BadRequestException e) {
      LOGGER.error("error while creating debit card  : ", e);
      FailureResponse failureResponse = new FailureResponse(e.getMessage(), "400", null);
      setAllMDCErrors(failureResponse, "VDC creation failed");
      PSStatUtils.incrementStats(PSStatUtils.Entity.CBS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
    } catch (Exception e) {
      LOGGER.error("error while creating debitcard  : ", e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
    }
    LOGGER.debug("Exiting createDebitCard");
    return issueCardResponse;
  }

  public IssueCardRequest createDCRequest(Map<String, Object> valueMap) {
    KycResponse kycResponse = (KycResponse) valueMap.get(StateMachineConstants.KYC_RESPONSE);
    UserDetailsResponse userDetailsResponse =
        (UserDetailsResponse) valueMap.get(StateMachineConstants.OAUTH_RESPONSE);
    String accountNumber = (String) valueMap.get(StateMachineConstants.ACCOUNT_NUMBER);
    IssueCardRequest issueCardRequest = new IssueCardRequest();
    issueCardRequest.setTypeCode((String) valueMap.get(StateMachineConstants.CARD_ACCOUNT_TYPE_CODE));
    issueCardRequest.setAccountNumber(accountNumber);
    issueCardRequest.setAddressLine1(kycResponse.getAddressDetails().getAddressLine1());
    issueCardRequest.setAddressLine2(kycResponse.getAddressDetails().getAddressLine2());
    issueCardRequest.setCity(kycResponse.getAddressDetails().getCity());
    issueCardRequest.setCountry("IND");
    issueCardRequest.setCustomerId(userDetailsResponse.getUserId());
    issueCardRequest.setDob(kycResponse.getCustomerDetails().getDob());

    DebitCardVariant debitCardVariant =
        (DebitCardVariant) valueMap.get(StateMachineConstants.CARD_TYPE);
    if (debitCardVariant != null) {
      issueCardRequest.setCardProductType(debitCardVariant.getTemplateId());
    } else {
      issueCardRequest.setCardProductType(CaPdcCardCodes.PVVI.getName());
    }
    CustomerDetails customerDetails=setCIFStandardName(kycResponse,FISParameterLengthLimit.MAX_CUST_NAME_LENGTH);

    issueCardRequest.setFirstName(customerDetails.getFirstName());
    issueCardRequest.setKycStatus("1");
    issueCardRequest.setLastName(customerDetails.getLastName());
    issueCardRequest.setPhone(userDetailsResponse.getUserInformation().getPhone());
    issueCardRequest.setPostalCode(kycResponse.getAddressDetails().getPostalCode());
    issueCardRequest.setState(kycResponse.getAddressDetails().getState());
    Set<String> messages = AppUtils.validate(issueCardRequest);
    if(AppUtils.validate(issueCardRequest)!=null){
      throw new BadRequestException(messages.toString().replace("[", "").replace("]", ""));
    } ;
    validateEmboss(issueCardRequest.getFirstName(),issueCardRequest.getLastName());
    return issueCardRequest;
  }

  private void validateEmboss(String firstName,String  lastName){
    int firstNameLength = 0;
    int lastNameLength = 0;
    if(StringUtils.isNotEmpty(firstName)){
      firstNameLength = firstName.length();
    }
    if(StringUtils.isNotEmpty(lastName)){
      lastNameLength = lastName.length();
    }
    int embossNameLength = firstNameLength+lastNameLength;
    if(embossNameLength>49){
      throw new BadRequestException("Name greater than 50");
    }
  }

  public boolean blockCard(String cardNumberAlias) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("Block debit card ,cardNumberAlias {}", cardNumberAlias);
    String txnId = MDC.get(Constants.REQUEST_ID);
    String url = hostUrl + CARD_ENDPOINT + "/" + cardNumberAlias + "/block";
    try {
      HttpHeaders requestHeaders = new HttpHeaders();
      requestHeaders.set(requestToken, txnId);
      HttpEntity<?> requestEntity = new HttpEntity<>(requestHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
//      ResponseEntity<JsonNode> responseEntity =
//          restClientUtil.makePutRequest(url, requestEntity, JsonNode.class);
      ResponseEntity<JsonNode> responseEntity = triggerRequest(url, requestEntity, JsonNode.class, PUT);
      long end = System.currentTimeMillis();

      if (responseEntity != null) {
        JsonNode jsonResponse = responseEntity.getBody();
        if (jsonResponse != null) {
          JsonNode responseStatus = jsonResponse.get("status");
          if (responseStatus == null
              || CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
            JsonNode jsonFailureResponse = jsonResponse.get("response");
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.BLOCK_CARD);


            FailureResponse cardFailureResponse =
                objectMapper.convertValue(jsonFailureResponse, FailureResponse.class);

//            if(null!=cardFailureResponse && cardFailureResponse.getErrorCode()!=null && cardFailureResponse.getErrorCode().equalsIgnoreCase(EC003)){
//              LOGGER.error("card already blocked at FIS end for alias " + cardNumberAlias +" "+cardFailureResponse);
//              PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
//                  PSStatUtils.Entity.Instance.FIS.BLOCK_CARD, end - start);
//              return true;
//            }
            LOGGER.error("Unable to block card [card alias: " + cardNumberAlias
                + ", transaction ID:" + txnId + "]" + cardFailureResponse);
            return false;
          } else {
            // this means operation is successful, no need to parse the response.
            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.BLOCK_CARD, end - start);
            return true;
          }
        } else {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.BLOCK_CARD);

        }
      } else {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.BLOCK_CARD);

      }
    } catch (HystrixRuntimeException hre) {

      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("Unable to block card [card alias: " + cardNumberAlias + ", transaction ID:"
          + txnId + "] : {}", response);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.BLOCK_CARD);
    } catch (Exception ex) {
      LOGGER.error("Unable to block card [card alias: " + cardNumberAlias + ", transaction ID:"
          + txnId + "]", ex);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.BLOCK_CARD);
    }
    LOGGER.debug("Exiting blockCard with cardNumberAlias {}", cardNumberAlias);
    return false;
  }


  public boolean enableInternationalUsage(String cardNumber, boolean isEnable) {
    //TODO enable international usage
    return true;
  }

  public boolean unblockCard(String cardNumberAlias) {
    validateFISApiSwitch(PUT);
    String txnId = MDC.get(Constants.REQUEST_ID);

    String url = hostUrl + CARD_ENDPOINT + "/" + cardNumberAlias + "/unblock";
    try {
      HttpHeaders requestHeaders = new HttpHeaders();
      requestHeaders.set(requestToken, txnId);
      HttpEntity<?> requestEntity = new HttpEntity<>(requestHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
//      ResponseEntity<JsonNode> responseEntity =
//          restClientUtil.makePutRequest(url, requestEntity, JsonNode.class);
      ResponseEntity<JsonNode> responseEntity = triggerRequest(url, requestEntity, JsonNode.class, PUT);
      long end = System.currentTimeMillis();

      if (responseEntity != null) {
        JsonNode jsonResponse = responseEntity.getBody();
        if (jsonResponse != null) {
          JsonNode responseStatus = jsonResponse.get("status");
          if (responseStatus == null
              || CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.UNBLOCK_CARD);
            JsonNode jsonFailureResponse = jsonResponse.get("response");

            FailureResponse cardFailureResponse =
                objectMapper.convertValue(jsonFailureResponse, FailureResponse.class);

//            if(null!=cardFailureResponse && cardFailureResponse.getErrorCode()!=null && cardFailureResponse.getErrorCode().equalsIgnoreCase(EC003)){
//              LOGGER.error("card already unblocked at FIS end for alias " + cardNumberAlias +" "+cardFailureResponse);
//              PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
//                  PSStatUtils.Entity.Instance.FIS.UNBLOCK_CARD, end - start);
//              return true;
//            }

            LOGGER.error("Unable to unblock card [card alias: " + cardNumberAlias
                + ", transaction ID:" + txnId + "]" + cardFailureResponse);
            return false;
          } else {
            // this means operation is successful, no need to parse the response.
            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.UNBLOCK_CARD, end - start);
            return true;
          }
        } else {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.UNBLOCK_CARD);
        }
      } else {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.UNBLOCK_CARD);
      }

    } catch (HystrixRuntimeException hre) {

      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("Unable to unblock card [card alias: " + cardNumberAlias + ", transaction ID:"
          + txnId + "] : {}", response);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.UNBLOCK_CARD);
    } catch (

    Exception ex) {
      LOGGER.error("Unable to unblock card [card alias: " + cardNumberAlias + ", transaction ID:"
          + txnId + "]", ex);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.UNBLOCK_CARD);
    }
    return false;
  }

  public BaseResponse getCardDetails(String cardNumberAlias) {
    validateFISApiSwitch(GET);
    LOGGER.debug("Fetch card details ,cardNumberAlias {}", cardNumberAlias);
    String txnId = MDC.get(Constants.REQUEST_ID);
    String url = hostUrl + CARD_ENDPOINT + "/" + cardNumberAlias;
    try {
      long start = System.currentTimeMillis();
      LOGGER.info("Requesting eis url {} ", url);
//      JsonNode jsonResponse =
//          restClientUtil.getApiResult(new URI(url), null, JsonNode.class, txnId);
      JsonNode jsonResponse = triggerGetApi(new URI(url), null, txnId);
      long end = System.currentTimeMillis();

      if (jsonResponse != null) {
        JsonNode responseStatus = jsonResponse.get("status");
        if (responseStatus == null
            || CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);

          JsonNode jsonFailureResponse = jsonResponse.get("response");
          FailureResponse cardFailureResponse =
              objectMapper.convertValue(jsonFailureResponse, FailureResponse.class);
          LOGGER.error("Unable to fetch card details [card alias: " + cardNumberAlias
              + ", transaction ID:" + txnId + "]" + cardFailureResponse);
          return cardFailureResponse;
        } else {
          LOGGER.info("card details successfully fetched from FIS for cardAlias: "+cardNumberAlias);
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS, end - start);
          JsonNode jsonSuccessResponse = jsonResponse.get("response");
          return objectMapper.convertValue(jsonSuccessResponse, CardDetailsResponse.class);
        }
      } else {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);

      }

    } catch (HystrixRuntimeException hre) {

      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("Unable to get card details [card alias: " + cardNumberAlias
          + ", transaction ID:" + txnId + "] : {}", response);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);
    } catch (Exception ex) {
      LOGGER.error("Unable to get card details [card alias: " + cardNumberAlias
          + ", transaction ID:" + txnId + "]", ex);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);

    }
    LOGGER.debug("exiting fetch card details for :" + cardNumberAlias);
    return null;
  }

  public BaseResponse getCardDetailsV2(String cardNumberAlias, String encryptionKey,
      String encryptionIV, String channel) {
    validateFISApiSwitch(GET);
    LOGGER.debug(
        "Fetch encrypted card details ,cardNumberAlias {} , encryptionKey {}, encryptionIV {}, channel {} ",
        cardNumberAlias, encryptionKey, encryptionIV, channel);
    String txnId = MDC.get(Constants.REQUEST_ID);
    String url = hostUrl + ENCRYPTED_CARD_DETAILS_ENDPOINT + "?cardAlias=" + cardNumberAlias;
    HttpHeaders httpHeaders = new HttpHeaders();
    httpHeaders.set(requestToken, txnId);
    httpHeaders.set("encryptionKey", encryptionKey);
    httpHeaders.set("encryptionIV", encryptionIV);
    httpHeaders.set("Channel", channel);
    try {
      long start = System.currentTimeMillis();
      LOGGER.info("Requesting eis url ENCRYPTED_CARD_DETAILS_ENDPOINT {} ", url);

      JsonNode jsonResponse = triggerGetApi(new URI(url), httpHeaders, txnId);
      long end = System.currentTimeMillis();

      if (jsonResponse != null) {
        JsonNode responseStatus = jsonResponse.get("status");
        if (responseStatus == null
            || CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);

          JsonNode jsonFailureResponse = jsonResponse.get("response");
          FailureResponse cardFailureResponse =
              objectMapper.convertValue(jsonFailureResponse, FailureResponse.class);
          LOGGER.error("Unable to fetch card details [card alias: " + cardNumberAlias
              + ", transaction ID:" + txnId + "]" + cardFailureResponse);
          return cardFailureResponse;
        } else {
          LOGGER
              .info("card details successfully fetched from FIS for cardAlias: " + cardNumberAlias);
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS, end - start);
          JsonNode jsonSuccessResponse = jsonResponse.get("response");
          String encryptedCardDetails =
              objectMapper.convertValue(jsonSuccessResponse, String.class);
          EncryptedCardDetailsResponse encryptedCardDetailsResponse =
              new EncryptedCardDetailsResponse();
          encryptedCardDetailsResponse.setResponse(encryptedCardDetails);
          encryptedCardDetailsResponse.setStatus(CustomResponse.Status.SUCCESS.name());
          return encryptedCardDetailsResponse;
        }
      } else {
        LOGGER.error("jsonResponse is null for cardAlias {}", cardNumberAlias);
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);

      }

    } catch (HystrixRuntimeException hre) {

      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("Unable to get card details [card alias: " + cardNumberAlias
          + ", transaction ID:" + txnId + "] : {}", response);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);
    } catch (Exception ex) {
      LOGGER.error("Unable to get card details [card alias: " + cardNumberAlias
          + ", transaction ID:" + txnId + "]", ex);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);

    }
    LOGGER.debug("exiting fetch card details for :" + cardNumberAlias);
    return null;
  }

  public BaseResponse getCardDetailsFromCustId(String custId) {
    validateFISApiSwitch(GET);
    LOGGER.debug("Fetch card details ,custId {}", custId);
    String txnId = MDC.get(Constants.REQUEST_ID);
    String url = hostUrl + CARD_ENDPOINT2 + "/" + custId + "/card";
    try {
      long start = System.currentTimeMillis();
      LOGGER.info("Requesting eis url {} ", url);
//      JsonNode jsonResponse =
//          restClientUtil.getApiResult(new URI(url), null, JsonNode.class, txnId);

      JsonNode jsonResponse = triggerGetApi(new URI(url), null, txnId);
      long end = System.currentTimeMillis();


      if (jsonResponse != null) {
        JsonNode responseStatus = jsonResponse.get("status");
        if (responseStatus == null
            || CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID);
          JsonNode jsonFailureResponse = jsonResponse.get("response");
          FailureResponse cardFailureResponse =
              objectMapper.convertValue(jsonFailureResponse, FailureResponse.class);
          //setAllMDCErrors(cardFailureResponse, "VDC enquiry Failed");
          LOGGER.error("Unable to fetch card details [custId : " + custId + ", transaction ID:"
              + txnId + "]" + cardFailureResponse);
          return cardFailureResponse;
        } else {
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID, end - start);
          JsonNode jsonSuccessResponse = jsonResponse.get("response");
          return objectMapper.convertValue(jsonSuccessResponse, DebitCardDetailsResponse.class);
        }
      } else {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID);
      }

    } catch (HystrixRuntimeException hre) {

      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error(
          "Unable to get card details [custId: " + custId + ", transaction ID:" + txnId + "] : {}",
          response);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID);
    } catch (Exception ex) {
      LOGGER.error(
          "Unable to get card details [custId: " + custId + ", transaction ID:" + txnId + "]", ex);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID);
    }
    LOGGER.debug("Couldn't fetch Card Details for custId {} ...returning null.", custId);
    return null;
  }

  public BaseResponse getCardDetailsFromCustIdAndAccountNumber(String custId,
      String accountNumber) {
    validateFISApiSwitch(GET);
    LOGGER.debug("Fetch card details, custId {}, accountNumber {}", custId, accountNumber);
    String requestId = MDC.get(Constants.REQUEST_ID);
    String url = hostUrl + CARD_ENDPOINT2 + "/" + custId + "/card?accountNumber=";
    try {
      long start = System.currentTimeMillis();
      LOGGER.info("Requesting eis url {} ", url + AppUtils.maskDataForLogs(accountNumber));
      // JsonNode jsonResponse =
      // restClientUtil.getApiResult(new URI(url), null, JsonNode.class, txnId);
      JsonNode jsonResponse = triggerGetApi(new URI(url + accountNumber), null, requestId);
      long end = System.currentTimeMillis();

      if (jsonResponse != null) {
        JsonNode responseStatus = jsonResponse.get("status");
        if (responseStatus == null
            || CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID_ACC_NUM);
          JsonNode jsonFailureResponse = jsonResponse.get("response");
          FailureResponse cardFailureResponse =
              objectMapper.convertValue(jsonFailureResponse, FailureResponse.class);
          // setAllMDCErrors(cardFailureResponse, "VDC enquiry Failed");
          LOGGER.error("Unable to fetch card details [custId : " + custId + ", accountNumber : "
              + AppUtils.maskDataForLogs(accountNumber) + ", request ID:" + requestId + "]"
              + cardFailureResponse);
          return cardFailureResponse;
        } else {
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID_ACC_NUM, end - start);
          JsonNode jsonSuccessResponse = jsonResponse.get("response");
          return objectMapper.convertValue(jsonSuccessResponse, DebitCardDetailsResponse.class);
        }
      } else {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID_ACC_NUM);
      }

    } catch (HystrixRuntimeException hre) {

      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("Unable to get card details [custId: " + custId + ", accountNumber : "
          + accountNumber + ", request ID:" + requestId + "] : {}", response);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID_ACC_NUM);
    } catch (Exception ex) {
      LOGGER.error("Unable to get card details [custId: " + custId + ", accountNumber : "
          + accountNumber + ", request ID:" + requestId + "]", ex);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS_CUSTID_ACC_NUM);
    }
    LOGGER.debug(
        "Couldn't fetch Card Details for custId {} and accountNumber {} ...returning null.", custId,
        accountNumber);
    return null;
  }

  public boolean createPhysicalDebitCard(Map<String, Object> valueMap, String cardNumber) {
    validateFISApiSwitch(POST);
    LOGGER.debug("Create physical debit card custId {}", (String)valueMap.get(StateMachineConstants.CUST_ID));
    String processId = MDC.get(Constants.REQUEST_ID);
    BaseResponse issuePDCardResponse = null;
    IssuePDCRequest issuePDCardRequest = createPDCIssueRequest(valueMap, cardNumber);
    try {
//      oauthProxy.isCustIdAllowed(issuePDCardRequest.getCustomerId());
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_PDC).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, processId);
      HttpEntity<IssuePDCRequest> httpEntity = new HttpEntity<>(issuePDCardRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} ", url);

      long start = System.currentTimeMillis();
//      ResponseEntity<JsonNode> responseEntity =
//          restClientUtil.makePostRequest(url, httpEntity, JsonNode.class);
      long end = System.currentTimeMillis();
      ResponseEntity<JsonNode> responseEntity = triggerRequest(url, httpEntity, JsonNode.class, POST);
      if (responseEntity != null) {
        JsonNode jsonResponse = responseEntity.getBody();
        if (jsonResponse != null) {
          JsonNode responseStatus = jsonResponse.get("status");
          JsonNode response = jsonResponse.get("response");
          if (responseStatus == null || response == null) {
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
            throw new DownStreamServerException("FIS is returning null repose or status");
          }
          if (CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(responseStatus.asText())) {
            LOGGER.info(
                "Got success response while creating Physical Debit Card [Cust-Id: {}]",
                valueMap.get(StateMachineConstants.CUST_ID));
            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD,
                end - start);
            if (null != response) {
              issuePDCardResponse =
                  AppUtils.fromJackson(response.toString(), IssDirectiveResponse.class);
              return true;
            }
          } else if (CustomResponse.Status.FAILURE.name()
              .equalsIgnoreCase(responseStatus.asText())) {
            FailureResponse failureResponse = AppUtils.getErrorFromEis(response);
            LOGGER.error(
                "Got failure response from EIS while creating Physical Debit Card [Cust-Id: {}]",
                valueMap.get(StateMachineConstants.CUST_ID));
            LOGGER.error(failureResponse.getMessage());
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
            if (failureResponse.getMessage()
                .equalsIgnoreCase("Virtual to Physical card request Already initiated")) {
              LOGGER.info("card already issued for custId {}",(String)valueMap.get(StateMachineConstants.CUST_ID));
              return true;
            } else {
              PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                  PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
              throw new DownStreamServerException(failureResponse.getMessage());
            }
          } else {
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
          }
        } else {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
        }
      } else {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
      }
    } catch (HystrixRuntimeException hre) {

      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("error while creating physical debitcard  : {}", response);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
    } catch (Exception e) {
      LOGGER.error("error while creating physical debitcard  : ", e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
    }
    LOGGER.debug("Exiting createDebitCard");
    return false;
  }

  private void setAllMDCErrors(FailureResponse failureResponse, String message){
    if(failureResponse !=null){
      MDC.put(Constants.MDC_ERROR_CODE, failureResponse.getErrorCode());
      if(message==null){
        MDC.put(Constants.MDC_ERROR_MSG, failureResponse.getMessage());
      }else{
        MDC.put(Constants.MDC_ERROR_MSG, message + " : " + failureResponse.getMessage());
      }
      if(failureResponse.getData()!=null){
        MDC.put(Constants.MDC_REMARK, failureResponse.getData().toString());
      }
    }
  }

  private IssuePDCRequest createPDCIssueRequest(Map<String, Object> valueMap, String cardNumber) {
    IssuePDCRequest issuePDCRequest = new IssuePDCRequest();
    PdcCustomization customization =
        ((ProductInitiationRequest) valueMap.get(StateMachineConstants.PRODUCT_INITIATION_REQUEST))
            .getOnboardingProductRequest().getPdcCustomization();
    issuePDCRequest.setOrderId(customization.getOrderId());
    issuePDCRequest.setAwbNumber((String) valueMap.get(StateMachineConstants.AWB_NUMBER));
    issuePDCRequest.setAddressLine0(customization.getDeliveryAddress().getAddressLine1());
    issuePDCRequest.setAddressLine1(customization.getDeliveryAddress().getAddressLine2());
    issuePDCRequest.setAddressLine2(customization.getDeliveryAddress().getAddressLine3());
    issuePDCRequest.setAddressLine3(customization.getDeliveryAddress().getAddressLine4());
    issuePDCRequest.setPinCode(customization.getDeliveryAddress().getPinCode());
    issuePDCRequest
            .setCity(AppUtils.transformCityLengthForFis(customization.getDeliveryAddress().getCity()));
    issuePDCRequest.setState(customization.getDeliveryAddress().getState());
    issuePDCRequest.setBranchCode((String) valueMap.get(StateMachineConstants.BRANCH_ID));
    issuePDCRequest.setPrintingVendor((String) valueMap.get(StateMachineConstants.VENDOR));

    DebitCardVariant requestedCardVariant = customization.getDebitCardVariant();
    if (requestedCardVariant.isPhoto()) {
      issuePDCRequest.setPhotoId((String) valueMap.get(StateMachineConstants.CUST_ID));
    }
    issuePDCRequest.setCardProduct(requestedCardVariant.getTemplateId());
    issuePDCRequest.setCardNumber(cardNumber);
    issuePDCRequest.setQrCode((String) valueMap.get(StateMachineConstants.WALLET_QR_CODE));
    issuePDCRequest.setCustomerId((String) valueMap.get(StateMachineConstants.CUST_ID));

    //RUPAY02 are not supported so commenting
   /* KycResponse kycResponse = (KycResponse) valueMap.get(StateMachineConstants.KYC_RESPONSE);
    if (PDCCardTemplateEnum.RUYPAY02.getId().equalsIgnoreCase(customization.getProductType())) {
      String aadhar = getAdharFromProofDetail(kycResponse.getProofDetails());
      issuePDCRequest.setAadharNo(aadhar);
    }*/
    issuePDCRequest.setMobileNumber(customization.getMobileNumber());
    String courierName = (String) valueMap.get(StateMachineConstants.SHIPPING_DESCRIPTION);
    LOGGER.info("pdc courier vendor for {} {} {}", valueMap.get(StateMachineConstants.CUST_ID),
        valueMap.get(StateMachineConstants.SHIPPER_ID), courierName);
    if (courierName != null && courierName.length() > MAX_COURIER_NAME_LENGTH) {
      courierName = courierName.substring(0, MAX_COURIER_NAME_LENGTH);
    }
    issuePDCRequest.setCourierVendor(courierName);
    AppUtils.validateObjectAndThrowException(issuePDCRequest);
    return issuePDCRequest;
  }

  private String getAdharFromProofDetail(List<ProofDetail> proofDetails) {
    String aadhar = null;
    for (ProofDetail proofDetail : proofDetails) {
      if (proofDetail.getDocumentCode().equalsIgnoreCase("aadhar")) {
        aadhar = proofDetail.getReferenceId();
      }
    }
    return aadhar;
  }

  /** Set Debit Card PIN (PUT /physicalcards/{card_number}/pin)
   * @param pin
   * @return
   */
  public BaseResponse setDebitCardPin(String pin, String cardNumber, String source, String custID) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("Set debit card PIN request for custId {}",custID);
    String requestId = MDC.get(Constants.REQUEST_ID);
    CustomPDCResponse<?> setDebitCardResponse;
    try {
      String setCardPinJsonRequest = generateRequestForSetPin(pin);
      LOGGER.info("setCardPinJsonRequest for custID, {}",custID);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_PDC).append(SET_PIN).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set("Request-Token", requestId);
      httpHeaders.set("Accept", "application/json");
      httpHeaders.set("Channel", source);
      httpHeaders.set("Card-Number", cardNumber);
      httpHeaders.setContentType(MediaType.APPLICATION_JSON);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
      ResponseEntity<String> responseEntity= sendHttpQrRequest(httpHeaders,url,setCardPinJsonRequest);
      long end = System.currentTimeMillis();

      LOGGER.info("setCardPinResponseEntity for custID  {} is:  {}",custID,maskPanInResponse(responseEntity));
      if(responseEntity != null){
        setDebitCardResponse = AppUtils.fromJson(responseEntity.getBody(), CustomPDCResponse.class);
          if (null != setDebitCardResponse && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(setDebitCardResponse.getStatus())) {
            LOGGER.info("Got success response while setting Debit Card Pin for custID {}",custID);
            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.SET_DEBIT_CARD_PIN, end - start);
            return setDebitCardResponse;

          }else if (null != setDebitCardResponse && !CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(setDebitCardResponse.getStatus())) {
            LOGGER.info("Got failure response from EIS while setting Debit Card Pin for custID {}",custID);
            if(setDebitCardResponse.getResponse()!=null){
              return AppUtils.convertValue(setDebitCardResponse.getResponse(), FailureResponse.class);
            }
          }
        }
      LOGGER.error("responseEntity for setDebitCardPin is null or response code is not 200 for custID {}",custID);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.SET_DEBIT_CARD_PIN);
      LOGGER.debug("Exiting setDebitCardPin for custId {}",custID);
      throw new DownStreamServerException(MessageConstants.PIN_SET_ERR.getMessage());
    }catch (Exception e) {
      LOGGER.error("exception while setting PIN for custID "+custID +" ",e);
      LOGGER.error(ERROR_WHILE_SETTING_DEBIT_CARD_PIN, e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.SET_DEBIT_CARD_PIN);
      throw e;

    }
  }

  /** FIS api to check PIN status (PUT /physicalcards/{card_number}/pin/validate)
   * @param pin
   * @param cardNumber
   * @return
   */
  public BaseResponse checkDebitCardPinStatus(String pin, String cardNumber, String source, String custId) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("Set debit card PIN request for custId {} ",custId);
    String requestId = MDC.get(Constants.REQUEST_ID);
    ResponseEntity<String> responseEntity=null;
    try {
      CustomPDCResponse<?> checkCardPinResponse;
      String checkCardPinJsonRequest = generateRequestForSetPin(pin);
      LOGGER.info("checkCardPinJsonRequest for custId {}",custId);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_PDC).append(CHECK_CARD_PIN).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set("Request-Token", requestId);
      httpHeaders.set("Accept", "application/json");
      httpHeaders.set("Channel", source);
      httpHeaders.set("Card-Number", cardNumber);
      httpHeaders.setContentType(MediaType.APPLICATION_JSON);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
      responseEntity= sendHttpQrRequest(httpHeaders,url,checkCardPinJsonRequest);
      long end = System.currentTimeMillis();
      LOGGER.info("checkCardPinResponseEntity for custId {} is {} : ", custId,maskPanInResponse(responseEntity));


      if(responseEntity != null && responseEntity.getStatusCode() == HttpStatus.OK){
        checkCardPinResponse = AppUtils.fromJson(responseEntity.getBody(),
            CustomPDCResponse.class);

          if (null != checkCardPinResponse
              && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(checkCardPinResponse.getStatus())) {
            LOGGER.info("Got success response while checking Debit Card Pin for custId {}",custId);
            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.CHECK_DEBIT_CARD_PIN, end - start);
            return checkCardPinResponse;

          }else if (null != checkCardPinResponse
              && !CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(checkCardPinResponse.getStatus())) {
            LOGGER.info("Got failure response from EIS while checking Debit Card Pin for custId {}",custId);
            if(checkCardPinResponse.getResponse()!=null){
              return AppUtils.convertValue(checkCardPinResponse.getResponse(), FailureResponse.class);
            }
          }
        }
      LOGGER.error("responseEntity for checkDebitCardPinStatus is null or response code is not 200 for  custId {}",custId);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.CHECK_DEBIT_CARD_PIN);
      LOGGER.debug("Exiting checkDebitCardPin for custId {}",custId);
      throw new DownStreamServerException(MessageConstants.PIN_CHECK_ERR.getMessage());
    } catch (Exception e) {
      LOGGER.error("error while Checking Card PIN Status  for custid {} : due to {} ",custId, e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CHECK_DEBIT_CARD_PIN);
      throw e;
    }
  }

  public BaseResponse updateCIF(Map<String, Object> valueMap,KycResponse kycResponse, String custId,DeliveryAddress deliveryAddress) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("Enter:Update CIF api for custId {}", custId);
    String requestId = MDC.get(Constants.REQUEST_ID);
    ResponseEntity<String> responseEntity=null;
    try {
      // oauthProxy.isCustIdAllowed(custId);
      CustomPDCResponse<?> cifUpdateResponse;
      String cifUpdateJsonRequest = generateRequestForCIFUpdate(kycResponse,valueMap,deliveryAddress);
      StringBuilder cifRequest=new StringBuilder(cifUpdateJsonRequest);
      int startIndex=cifUpdateJsonRequest.indexOf("pan");
      cifRequest.replace(startIndex+5, startIndex+22, "\"xxxxx");
      LOGGER.info("cifUpdateJsonRequest for custId {} is {} : ",custId, cifRequest);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_PDC).append("/").append(custId).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set("Request-Token", requestId);
      httpHeaders.set("Accept", "application/json");
      httpHeaders.setContentType(MediaType.APPLICATION_JSON);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
      responseEntity= sendHttpQrRequest(httpHeaders,url,cifUpdateJsonRequest);
      long end = System.currentTimeMillis();

      if(responseEntity != null && responseEntity.getStatusCode() == HttpStatus.OK){
        cifUpdateResponse = AppUtils.fromJson(responseEntity.getBody(),
            CustomPDCResponse.class);

          if (null != cifUpdateResponse
              && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(cifUpdateResponse.getStatus())) {
            LOGGER.info("Got success response while Updating CIF for custId {}",custId);
            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.CIF_UPDATE, end - start);
            return cifUpdateResponse;

          }else if (null != cifUpdateResponse
              && !CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(cifUpdateResponse.getStatus())) {
            LOGGER.info("Got failure response from EIS while Updating CIF for custId {}",custId);
            if(cifUpdateResponse.getResponse()!=null){
              PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                  PSStatUtils.Entity.Instance.FIS.CIF_UPDATE);
              return AppUtils.convertValue(cifUpdateResponse.getResponse(), FailureResponse.class);
            }
          }
        }
      LOGGER.error("responseEntity for updateCIF is null or response code is not 200 for custid {}",custId);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.CIF_UPDATE);
      LOGGER.debug("Exiting Update CIF api for custid {}",custId);
      throw new DownStreamServerException(MessageConstants.UPDATE_CIF_ERR.getMessage());
    } catch (Exception e) {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CIF_UPDATE);
      LOGGER.error("error while UpdateCIF Status for custId {} due to {} " ,custId, e);
      throw e;
    }
  }


  /** Common Rest API to send PUT request for SET and CheckCardAPI
   * @param httpHeaders
   * @param url
   * @param qrCodeMapJsonRequest
   * @return
   */
  private ResponseEntity<String> sendHttpQrRequest(HttpHeaders httpHeaders, String url,
      String qrCodeMapJsonRequest) {
    HttpEntity<String> httpEntity = new HttpEntity<>(qrCodeMapJsonRequest, httpHeaders);
    // return restClientUtil.makePutRequest(url, httpEntity, String.class);
    ResponseEntity<String> responseEntity = null;
    CustomPDCResponse<?> failureResponseObject;
    try {
      responseEntity = triggerRequest(url, httpEntity, String.class, PUT);
    } catch (HystrixRuntimeException hre) {
      if (hre.getCause() instanceof HttpStatusCodeException) {
        HttpStatusCodeException codeException = ((HttpStatusCodeException) hre.getCause());
        LOGGER.error("exception received from FIS with msg and status code {}, {}, for url {}",codeException.getResponseBodyAsString(),codeException.getStatusCode(), url);
        failureResponseObject = AppUtils.fromJson(codeException.getResponseBodyAsString(), CustomPDCResponse.class);
        if(failureResponseObject!=null && failureResponseObject.getResponse()!=null){
          FailureResponse failureResponse=AppUtils.convertValue(failureResponseObject.getResponse(), FailureResponse.class);
          if(codeException.getStatusCode()==HttpStatus.BAD_REQUEST){
            throw new BadRequestException(failureResponse.getMessage());
          }
          throw new DownStreamServerException(failureResponse.getMessage());
        }
      }
      //handling error like IO error or timeout
      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("error (could be IO/timeout) while PUT request for SET/CheckCardAPI/cardReissuance/updateCif  : {}, for url {}", response,url);
      throw new DownStreamServerException();
    } catch (Exception e) {
      LOGGER.error("error while PUT request for SET/CheckCardAPI/cardReissuance/updateCif  : ", e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
    }
    return responseEntity;
  }

  /**
   * Create SET_CARD_PIN  Request
   * @param encryptedPin
   * @return
   */
  private String generateRequestForSetPin(String encryptedPin) {
    PDCPinSetRequest pdcPinRequest = new PDCPinSetRequest();
    pdcPinRequest.setEncryptedPin(encryptedPin);
    pdcPinRequest.setCardPinType("1"); // Mandatory for Set Card PIN
    return AppUtils.toJson(pdcPinRequest);

  }

  /**Create Request for CIF update
   * @param kycResponse
   * @return
   */
  private String generateRequestForCIFUpdate(KycResponse kycResponse,Map<String, Object> valueMap,DeliveryAddress deliveryAddress){
    UpdateCustomerRequest updateCustomerRequest=new UpdateCustomerRequest();
    if(kycResponse!=null && kycResponse.getCustomerDetails()!=null && kycResponse.getAddressDetails()!=null){
      String dob=DateUtil.getCustomDateFormat(kycResponse.getCustomerDetails().getDob(), Constants.EXTERNAL_PARAMS.DATE_FORMAT.KYC, Constants.EXTERNAL_PARAMS.DATE_FORMAT.FIS);
     dob=AppUtils.validateLength(dob,FISParameterLengthLimit.MAX_FF_DOB_LENGTH);
     if(dob.equalsIgnoreCase(".")){
       dob="1999-12-12"; // rare case scenario, if we get DOB empty/null by chance from kyc
     }
    updateCustomerRequest.setDob(dob);

    CustomerDetails customerDetails=setCIFStandardName(kycResponse,FISParameterLengthLimit.MAX_CUST_NAME_LENGTH);
    updateCustomerRequest.setFirstName(customerDetails.getFirstName());
    updateCustomerRequest.setLastName(customerDetails.getLastName());
    LOGGER.debug("CIF Request FirstName is: "+customerDetails.getFirstName() +" and lastName is: "+customerDetails.getLastName());

    String embossedName=getEmbossedName(kycResponse,FISParameterLengthLimit.MAX_CUST_NAME_LENGTH);
    if(embossedName.length()>FISParameterLengthLimit.MAX_CUST_NAME_LENGTH){
      embossedName=embossedName.substring(0, FISParameterLengthLimit.MAX_CUST_NAME_LENGTH);
    }
    LOGGER.info("embossed name for custID: "+valueMap.get(StateMachineConstants.CUST_ID) +" is "+ AppUtils.maskDataForLogs(embossedName));
    valueMap.put(StateMachineConstants.CUST_FINAL_NAME, embossedName);
    updateCustomerRequest.setEmbossName(embossedName);

    updateCustomerRequest.setGender(AppUtils.validateLength(kycResponse.getCustomerDetails().getGender(),FISParameterLengthLimit.MAX_FF_GENDER_LENGTH));

    String cardAliasNumber = (String) valueMap.get(StateMachineConstants.CARD_NUMBER);
    BaseResponse baseResponse = getCardDetails(cardAliasNumber);
    String cardNo = getCardNumber(baseResponse);
    updateCustomerRequest.setPan(cardNo);
    List<AddressDetails> addressList=new ArrayList<>();

    AddressDetails deliveryAddressDetail=new AddressDetails();
    deliveryAddressDetail.setAddressLine1(deliveryAddress.getAddressLine1());
    deliveryAddressDetail.setAddressLine2(deliveryAddress.getAddressLine2());
    deliveryAddressDetail.setAddressLine3(deliveryAddress.getAddressLine3());
    deliveryAddressDetail.setAddressLine4(deliveryAddress.getAddressLine4());
    deliveryAddressDetail.setCity(AppUtils.transformCityLengthForFis(deliveryAddress.getCity()));
    deliveryAddressDetail.setPostalCode(AppUtils.validateLength(deliveryAddress.getPinCode(),FISParameterLengthLimit.MAX_FF_POSTAL_CODE_LENGTH));
    deliveryAddressDetail.setState(AppUtils.validateLength(deliveryAddress.getState(),FISParameterLengthLimit.MAX_FF_STATE_LENGTH));
    deliveryAddressDetail.setAddressType(1);

    addressList.add(deliveryAddressDetail);
    AddressDetails kycAddressDetail=new AddressDetails();
    com.commons.dto.response.AddressDetails addressDetailKyc=kycResponse.getAddressDetails();
    updateDeliveryAddress(kycAddressDetail,addressDetailKyc,kycResponse.getCustomerDetails().getCustomerId(), FISParameterLengthLimit.MAX_KYC_ADDRESS_LINE_LENGTH);
    kycAddressDetail.setCity(AppUtils.validateLength(addressDetailKyc.getCity(),FISParameterLengthLimit.MAX_FF_CITY_LENGTH));
    kycAddressDetail.setPostalCode(AppUtils.validateLength(addressDetailKyc.getPostalCode(),FISParameterLengthLimit.MAX_FF_POSTAL_CODE_LENGTH));
    kycAddressDetail.setState(AppUtils.validateLength(addressDetailKyc.getState(),FISParameterLengthLimit.MAX_FF_STATE_LENGTH));
    kycAddressDetail.setAddressType(2);

    addressList.add(kycAddressDetail);
    updateCustomerRequest.setAddressDetails(addressList);
    return AppUtils.toJson(updateCustomerRequest);
    }
    throw new StateMachineException("Either KYCResponse is null or KYC customerDetail is empty or KYC addressDetail is empty");

  }
  /**Validate Card Number from FIS
   * @param cardResponse
   * @return
   */
  public String getCardNumber(BaseResponse cardResponse) {
    if (cardResponse == null) {
      throw new DownStreamServerException("Could not fetch card detail from EIS");
    } else if (!cardResponse.getClass().equals(CardDetailsResponse.class)) {
      throw new DownStreamServerException(((FailureResponse) cardResponse).getMessage());
    }
    CardDetailsResponse cardDetailsResponse = (CardDetailsResponse) cardResponse;
    String cardNumber = cardDetailsResponse.getPan();
    if (StringUtils.isEmpty(cardNumber)) {
      throw new StateMachineException("card number returned empty from EIS check PIN API");
    }
    return cardNumber;
  }

  /**Set the first Name and lastName based on PDC name length format
   * FirstName + LastName length <=25
   * then correct format
   * @param kycResponse
   * @return
   */
  public CustomerDetails setCIFStandardName(KycResponse kycResponse, int length) {

    CustomerDetails customerDetails = new CustomerDetails();
      String firstName =kycResponse.getCustomerDetails().getFirstName();
      String lastName =kycResponse.getCustomerDetails().getLastName();
      String finalName;
      if(kycResponse.getCustomerDetails()!=null){
      LOGGER.debug("KYC firstName is: "+firstName +" and Last name is: "+lastName +" for custID: "+kycResponse.getCustomerDetails().getCustomerId());
      }
      if (firstName == null || firstName.isEmpty() ||firstName.trim().isEmpty()) {
        customerDetails.setFirstName(".");
      }
      else{
      firstName = firstName.trim().replaceAll(" +", " ");
      customerDetails.setFirstName(firstName);
          if (firstName.length() > length) {
            finalName=getCustomName(firstName,length);
              if(finalName.length()>length){
                finalName=finalName.substring(0,length);
              }
              customerDetails.setFirstName(finalName);
          }
      }
      if (lastName == null || lastName.isEmpty()
          || lastName.equalsIgnoreCase(Constants.LAST_NAME_UNKNOWN)||lastName.trim().isEmpty()) {
          customerDetails.setLastName(".");
      } else {
            lastName = lastName.trim().replaceAll(" +", " ");
            customerDetails.setLastName(lastName);
                if (lastName.length() > length) {
                    finalName=getCustomName(lastName,length);
                      if(finalName.length()>length){
                          finalName=finalName.substring(0,length);
                      }
                      customerDetails.setLastName(finalName);
                }
        }

      return customerDetails;

  }


  /** create embossing Name using the below logic:
   * Assume Firstname=F, lastname=L
   * if :F+L<=25---->OK
   * else make initials of L from last words
   * @param kycResponse
   * @param length
   * @return
   */
  public String getEmbossedName(KycResponse kycResponse, int length) {

      String firstName =kycResponse.getCustomerDetails().getFirstName();
      String lastName =kycResponse.getCustomerDetails().getLastName();

      if (lastName == null || lastName.isEmpty()
          || lastName.equalsIgnoreCase(Constants.LAST_NAME_UNKNOWN)||lastName.trim().isEmpty()) {
        lastName = null;
      }else{
        lastName = lastName.trim();
      }
      if (firstName == null || firstName.isEmpty()||firstName.trim().isEmpty()) {
        firstName=".";
      }else{
        firstName=firstName.trim();
      }

      String str = lastName != null ? firstName + " " + lastName : firstName;
      str=str.trim().replaceAll(" +", " ");
      if(str.length()<=length){
        return str;
      }
      else{
        return getCustomName(str, length);
      }
  }


  /** Get Custom name based on length provided
   * e.g. : Name="xxxx yyyyy zzzz kkkkk"  length=15
   * output: xxxx yyyyy Z K
   * @param name
   * @param len
   * @return
   */
  public static String getCustomName(String name, int len){
    if (name.length() > len) {
      String str[] = name.split(" ");
      for (int i = str.length - 1; i >= 0; i--) {
        String initial = String.valueOf(str[i].charAt(0)).toUpperCase().trim();
        str[i] = initial;
        if(checkLength(str)<=len){
         name = String.join(" ", str);
         break;
        }
      }
    }
        return name;
  }

  private static int checkLength(String[] str) {
    int count=0;
    for(String s:str){
      count=count+s.length();
    }
    count=count+str.length-1;
    return count;
  }

  /** Pan no masked in responseEntity
   * @param responseEntity
   * @return
   */
  private StringBuilder maskPanInResponse(ResponseEntity<String> responseEntity) {
    if(responseEntity!=null){

    StringBuilder responseEntityStr=new StringBuilder(responseEntity.toString());
    if(responseEntityStr.toString().contains("pan")){
    int startIndex=responseEntityStr.indexOf("pan");
    responseEntityStr.replace(startIndex+5, startIndex+22, "\"xxxxxxxxxxxxxxxx");
    }

    return responseEntityStr;
    }
     return null;
  }


  /**update Delivery address based on standard format
   * @param addressDetails
   */
  private void updateDeliveryAddress(AddressDetails addressDetailToSent,com.commons.dto.response.AddressDetails addressDetails, String custID, int length) {
    if (addressDetails != null && addressDetails.getAddressLine1() != null) {
      String fullAddress = addressDetails.getAddressLine1();
      LOGGER.debug("KYC Address line 1: "+fullAddress);
      if (addressDetails.getAddressLine2() != null) {
        fullAddress = fullAddress + addressDetails.getAddressLine2();
        LOGGER.debug("KYC Address line 1 +line2 : "+fullAddress);
      }
      if (fullAddress != null && fullAddress.length() >= length) {
        addressDetailToSent.setAddressLine1(fullAddress.substring(0, length));
        LOGGER.debug("CIF request KYC address line1 :"+fullAddress.substring(0, length));
        String secndLineAdd =
            fullAddress.substring(addressDetailToSent.getAddressLine1().length(),
                fullAddress.length());
        if (secndLineAdd.length() >= length) {
          addressDetailToSent.setAddressLine2(secndLineAdd.substring(0, length));
          LOGGER.debug("CIF request KYC address line2 :"+secndLineAdd.substring(0, length));
          String thirdLineAdd =
              secndLineAdd.substring(addressDetailToSent.getAddressLine2().length(),
                  secndLineAdd.length());
          if (thirdLineAdd.length() >= length) {
            addressDetailToSent.setAddressLine3(thirdLineAdd.substring(0, length));
            LOGGER.debug("CIF request KYC address line3 :"+thirdLineAdd.substring(0, length));
          } else {
            addressDetailToSent.setAddressLine3(thirdLineAdd);
            LOGGER.debug("CIF request KYC address line3 :"+thirdLineAdd);
          }
        } else {
          addressDetailToSent.setAddressLine2(secndLineAdd);
          LOGGER.debug("CIF request KYC address line2 :"+secndLineAdd);
        }
      } else {
        addressDetailToSent.setAddressLine1(fullAddress);
        LOGGER.debug("CIF request KYC address line1 :"+fullAddress);
      }
    }
    else{
      LOGGER.error("either address is null or address line 1 is null from KYC");
    }
  }


  public boolean changeDebitCardStatus(String cardAliasNumber, DebitCardStatusEnum cardStatus) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("Block debit card ,cardNumberAlias {}", cardAliasNumber);
    String txnId = MDC.get(Constants.REQUEST_ID);
    String url = new StringBuilder(hostUrl).append(CHANGE_CARD_STATUS).append("/")
        .append(cardAliasNumber).toString();
    try {
      HttpHeaders requestHeaders = new HttpHeaders();
      requestHeaders.set(requestToken, txnId);
      ChangeDebitCardStatus request = new ChangeDebitCardStatus();
      request.setUpdateCardStatus(cardStatus.name());
      HttpEntity<?> requestEntity = new HttpEntity<>(request, requestHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
//      ResponseEntity<JsonNode> responseEntity =
//          restClientUtil.makePutRequest(url, requestEntity, JsonNode.class);
      ResponseEntity<JsonNode> responseEntity =
          triggerRequest(url, requestEntity, JsonNode.class, PUT);
      long end = System.currentTimeMillis();

      if (responseEntity != null) {
        JsonNode jsonResponse = responseEntity.getBody();
        if (jsonResponse != null) {
          JsonNode responseStatus = jsonResponse.get("status");
          if (responseStatus == null
              || CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
            JsonNode jsonFailureResponse = jsonResponse.get("response");
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CHANGE_STATUS_DEBIT_CARD);
            FailureResponse cardFailureResponse =
                objectMapper.convertValue(jsonFailureResponse, FailureResponse.class);
//            if ("EC003".equalsIgnoreCase(cardFailureResponse.getErrorCode())) {
//              return true;
//            }
            LOGGER.error("Unable to block card [card alias: " + cardAliasNumber+
                ", transaction ID:" + txnId + "]" + cardFailureResponse);
            return false;
          } else if (CustomResponse.Status.SUCCESS.name()
              .equalsIgnoreCase(responseStatus.asText())) {
            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.CHANGE_STATUS_DEBIT_CARD, end - start);
            return true;
          }
          else {
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CHANGE_STATUS_DEBIT_CARD);
          }
        } else {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.CHANGE_STATUS_DEBIT_CARD);

        }
      } else {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.CHANGE_STATUS_DEBIT_CARD);

      }
    } catch (HystrixRuntimeException hre) {

      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("Unable to block card [card alias: " + cardAliasNumber + ", transaction ID:"
          + txnId + "] : {}", response);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CHANGE_STATUS_DEBIT_CARD);
    } catch (Exception ex) {
      LOGGER.error("Unable to block card [card alias: " + cardAliasNumber+  ", transaction ID:"
          + txnId  +"]", ex);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CHANGE_STATUS_DEBIT_CARD);
    }
    LOGGER.debug("Exiting blockCard with cardNumberAlias {}", cardAliasNumber);
    return false;
  }

  /** EIS API for Card Reissuance
   * @param pan
   * @param custId
   * @param cardProduct
   * @return
   */
  public BaseResponse cardReIssuance(String pan,String custId, String cardProduct) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("Enter:cardReissuance for custId {} ", custId);
    String requestId = MDC.get(Constants.REQUEST_ID);
    ResponseEntity<String> responseEntity=null;
    try {
//      oauthProxy.isCustIdAllowed(custId);
      CustomPDCResponse<?> cardReissuanceResponse;
      ReIssuanceRequest reIssuanceRequest = new ReIssuanceRequest();
      reIssuanceRequest.setCardProduct(cardProduct);
      String cardReIssueJsonRequest =  AppUtils.toJson(reIssuanceRequest);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_PDC).append("/").append("reissuance").toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set("Request-Token", requestId);
      httpHeaders.set("Accept", "application/json");
      httpHeaders.set("Card-Number", pan);
      httpHeaders.setContentType(MediaType.APPLICATION_JSON);
      LOGGER.info("Requesting eis url  for custId {} is {} ", custId,url);
      long start = System.currentTimeMillis();
      responseEntity= sendHttpQrRequest(httpHeaders,url,cardReIssueJsonRequest);
      long end = System.currentTimeMillis();

      if(responseEntity != null && responseEntity.getStatusCode() == HttpStatus.OK){
        cardReissuanceResponse = AppUtils.fromJson(responseEntity.getBody(),
            CustomPDCResponse.class);

          if (null != cardReissuanceResponse
              && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(cardReissuanceResponse.getStatus())) {
            LOGGER.info("Got success response for card Reissuance for custID: {}",custId);
            PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
                PSStatUtils.Entity.Instance.FIS.CARD_REISSUANCE, end - start);
            IssDirectiveResponse response=AppUtils.convertValue(cardReissuanceResponse.getResponse(), IssDirectiveResponse.class);
            if(response.getCardDetResponse()!=null){
            LOGGER.info("received success response from FIS for card reissaunce of cardAlias: "+ response.getCardDetResponse().getAlias());
            }
            return response;

          }else if (null != cardReissuanceResponse
              && !CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(cardReissuanceResponse.getStatus())) {
            LOGGER.info("Got failure response from EIS for card Reissuance for custID: {} ",custId);
            if(cardReissuanceResponse.getResponse()!=null){
              PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                  PSStatUtils.Entity.Instance.FIS.CARD_REISSUANCE);
              return AppUtils.convertValue(cardReissuanceResponse.getResponse(), FailureResponse.class);
            }
          }
        }
      LOGGER.error("responseEntity for updateCIF is null or response code is not 200 for custid {}",custId);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CARD_REISSUANCE);
      LOGGER.debug("Exiting card Reissuance api for custid {}",custId);
      throw new DownStreamServerException(MessageConstants.CARD_REISSUED_FAILURE.getMessage());
    } catch (Exception e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CARD_REISSUANCE);
      LOGGER.error("error while Reissuance Status for custId {} due to {} " ,custId, e);
      throw e;
    }
  }

  /**
   * Create CARD_REISSUANCE  Request
   * @param encryptedPin
   * @return
   */
  private String generateRequestForCardReissuance() {
    ReIssuanceRequest reIssuanceRequest = new ReIssuanceRequest();
    reIssuanceRequest.setCardProduct("PGP1");
    return AppUtils.toJson(reIssuanceRequest);

  }

  public ApiResponse createIdcInvoice(IdcInvoiceRequest idcInvoiceRequest, String validatedAddress,String vendor) {
    validateFISApiSwitch(POST);
    LOGGER.debug("Creating idc invoice for {}", idcInvoiceRequest);
    String processId = MDC.get(Constants.REQUEST_ID);
    ApiResponse apiResponse = null;
    try {
      CardOrderRequest  cardOrderRequest = new CardOrderRequest();
      cardOrderRequest.setBrnCode(idcInvoiceRequest.getBranchId());
      cardOrderRequest.setCrdProduct(idcInvoiceRequest.getCardTemplateId());
      cardOrderRequest.setQuantity(String.valueOf(idcInvoiceRequest.getNumberOfCards()));
      cardOrderRequest.setWhoSet(idcInvoiceRequest.getAgentId());
      cardOrderRequest.setDeliveryAddress(validatedAddress);
      cardOrderRequest.setPrintingVendor(vendor);
      cardOrderRequest.setUniversityName(idcInvoiceRequest.getUniversityName());
      AppUtils.validateObjectAndThrowException(cardOrderRequest);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_IDC).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, processId);
      HttpEntity<CardOrderRequest> httpEntity = new HttpEntity<>(cardOrderRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} and requestBody is {}", url,
          AppUtils.toJson(cardOrderRequest));
      long start = System.currentTimeMillis();
//      ResponseEntity<JsonNode> responseEntity =
//          restClientUtil.makePostRequest(url, httpEntity, JsonNode.class);
      ResponseEntity<JsonNode> responseEntity = triggerRequest(url, httpEntity, JsonNode.class, POST);
      long end = System.currentTimeMillis();
      if (responseEntity != null) {
        apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(), CardOrderResponse.class,
            FailureResponse.class);
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.CREATE_IDC_INVOICE, end - start);
      }
    } catch (HystrixRuntimeException hre) {
      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error(
          "error while generating insta card invoice for given request {} exception : {} : {}",
          idcInvoiceRequest, response, hre);
      apiResponse = AppUtils.hystrixExceptionApiResponse(hre);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_IDC_INVOICE);
    } catch (HttpClientErrorException e) {
      apiResponse = new ApiResponse();
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_IDC_INVOICE);
    } catch (BadRequestException e) {
      LOGGER.error("error while generating insta card invoice for given request {}",
          idcInvoiceRequest, e);
      apiResponse = new ApiResponse();
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_IDC_INVOICE);
      AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
    } catch (Exception e) {
      LOGGER.error("error while generating insta card invoice for given request : {}",
          idcInvoiceRequest, e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_IDC_INVOICE);
    }
    if (apiResponse == null || !apiResponse.isSuccess()) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.CREATE_IDC_INVOICE);
      if(apiResponse!=null){
        LOGGER.error("error while generating insta card invoice for given request {} due to {}",idcInvoiceRequest, apiResponse.getResponse() );
      }
    } else {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.CREATE_IDC_INVOICE);
    }
    return apiResponse;
  }

  public ApiResponse idcQrMappingInquiry(String invoiceNumber) {
    validateFISApiSwitch(GET);
    String processId = MDC.get(Constants.REQUEST_ID);
    ApiResponse apiResponse = null;
    try {
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_IDC).append("/").append("invoices").append("/")
          .append(invoiceNumber).append("/").append("status").toString();
      URI uri = new URI(url);
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, processId);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
//      JsonNode jsonNode = restClientUtil.getApiResult(uri, httpHeaders, JsonNode.class, processId);
      JsonNode jsonNode = triggerGetApi(uri, httpHeaders, processId);
      apiResponse =
          AppUtils.parseJsonResponse(jsonNode, QrMappingResponse.class, FailureResponse.class);
      PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING_INQUIRY,
          System.currentTimeMillis() - start);
    } catch (HystrixRuntimeException hre) {
      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("error while insta card qr mapping account for given invoiceNumber {} {} : {}",
          invoiceNumber, response, hre);
      apiResponse = AppUtils.hystrixExceptionApiResponse(hre);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING_INQUIRY);
    } catch (HttpClientErrorException e) {
      apiResponse = new ApiResponse();
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING_INQUIRY);
    } catch (BadRequestException e) {
      LOGGER.error("error while insta card qr mapping account for given invoiceNumber : {}", invoiceNumber, e);
      apiResponse = new ApiResponse();
      AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING_INQUIRY);
    } catch (Exception e) {
      LOGGER.error(
          "error while insta card qr mapping account for given invoiceNumber : {}", invoiceNumber, e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING_INQUIRY);
    }
    if (apiResponse != null && apiResponse.isSuccess()) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING_INQUIRY);
    }
    return apiResponse;
  }

  public ApiResponse idcQrMapping(String invoiceNumber, String qrCode,
      BarcodeDetails barcodeDetails) {
    validateFISApiSwitch(POST);
    int failedCount = 0;
    String processId = MDC.get(Constants.REQUEST_ID);
    ApiResponse apiResponse = null;
    while (failedCount < 4) {
      LOGGER.debug("insta card Qr mapping request with invoiceNumber {} and QrCode {}",
          invoiceNumber, qrCode);
      try {
        if (failedCount > 0) {
          Thread.sleep(1000);
        }
        StringBuilder sb = new StringBuilder(hostUrl);
        String url = sb.append(ISSUE_IDC).append("/").append("invoices").append("/")
            .append(invoiceNumber).append("/").append("qr-mappings/").toString();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(requestToken, processId);
        QrMappingRequest qrMappingRequest = new QrMappingRequest(qrCode);
        if (barcodeDetails != null) {
          qrMappingRequest.setBarCode(barcodeDetails.getBarCode());
        }
        // AppUtils.validateObjectAndThrowException(qrMappingRequest);
        HttpEntity<QrMappingRequest> httpEntity = new HttpEntity<>(qrMappingRequest, httpHeaders);
        LOGGER.info("Requesting eis url {} ", url);
        long start = System.currentTimeMillis();
//        ResponseEntity<JsonNode> responseEntity =
//            restClientUtil.makePostRequest(url, httpEntity, JsonNode.class);
        ResponseEntity<JsonNode> responseEntity = triggerRequest(url, httpEntity, JsonNode.class, POST);
        long end = System.currentTimeMillis();
        if (responseEntity != null) {
          apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(),
              QrMappingResponse.class, FailureResponse.class);
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING,
              end - start);
        }
        break;
      } catch (HystrixRuntimeException hre) {
        String response = fetchHystrixExceptionMessage(hre);
        LOGGER.error("Error Response for Idc QR mapping {} {} : {}", invoiceNumber, response, hre);
        failedCount++;
        if (failedCount == 4) {
          apiResponse = AppUtils.hystrixExceptionApiResponse(hre);
        }
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING);
      } catch (HttpClientErrorException e) {
        LOGGER.error("Error Response for Idc QR mapping {}" + e.getResponseBodyAsString());
        failedCount++;
        if (failedCount == 4) {
          apiResponse = new ApiResponse();
          AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
        }
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING);
      } catch (BadRequestException e) {
        LOGGER.error("Error while IDC QR mapping for invoice number " + invoiceNumber, e);
        failedCount++;
        if (failedCount == 4) {
          apiResponse = new ApiResponse();
          AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
        }
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING);
      } catch (Exception e) {
        LOGGER.error("error while insta card Qr mapping for invoice number " + invoiceNumber, e);
        failedCount++;
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING);
      }
    }
    if (apiResponse != null && apiResponse.isSuccess()) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.IDC_QR_MAPPING);
    }
    return apiResponse;
  }

  public ApiResponse idcCardReplacement(String custId, String oldCardNumber, String CardAlias,
      String statCode, String cardProduct) {
    validateFISApiSwitch(POST);
    LOGGER.debug("insta card Replacement request for custId {} , cardProduct {} , statCode {}",
        custId, cardProduct, statCode);
    String processId = MDC.get(Constants.REQUEST_ID);
    ApiResponse apiResponse = null;
    try {
//      oauthProxy.isCustIdAllowed(custId);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_IDC).append("/").append(CardAlias).append("/")
          .append("replacement").toString();

      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, processId);
      InstaCardReplacement instaCardReplacement =
          new InstaCardReplacement(custId, oldCardNumber, cardProduct, statCode);
     // AppUtils.validateObjectAndThrowException(instaCardReplacement);
      HttpEntity<InstaCardReplacement> httpEntity =
          new HttpEntity<>(instaCardReplacement, httpHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
//      ResponseEntity<JsonNode> responseEntity =
//          restClientUtil.makePostRequest(url, httpEntity, JsonNode.class);
      ResponseEntity<JsonNode> responseEntity = triggerRequest(url, httpEntity, JsonNode.class, POST);
      long end = System.currentTimeMillis();
      if (responseEntity != null) {
        apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(),
            InstaCardReplacementResponse.class, FailureResponse.class);
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.IDC_CARD_REPLACEMENT,
            end - start);
      }
    } catch (HystrixRuntimeException hre) {
      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("error while insta card Replacement for given custId {} {} : {}", custId,
          response, hre);
      apiResponse = AppUtils.hystrixExceptionApiResponse(hre);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CARD_REPLACEMENT);
    } catch (HttpClientErrorException e) {
      apiResponse = new ApiResponse();
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CARD_REPLACEMENT);
    } catch (BadRequestException e) {
      LOGGER.error("error while insta card Replacement for given custId : {}", custId, e);
      apiResponse = new ApiResponse();
      AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CARD_REPLACEMENT);
    } catch (Exception e) {
      LOGGER.error("error while insta card Replacement for given custId : {}", custId, e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CARD_REPLACEMENT);
    }
    if (apiResponse != null && apiResponse.isSuccess()) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.IDC_CARD_REPLACEMENT);
    }
    return apiResponse;
  }

  public ApiResponse issueIDC(Map<String, Object> valueMap) {
    validateFISApiSwitch(POST);
    KycResponse kycResponse = (KycResponse) valueMap.get(StateMachineConstants.KYC_RESPONSE);
    UserDetailsResponse userDetailsResponse =
        (UserDetailsResponse) valueMap.get(StateMachineConstants.OAUTH_RESPONSE);

    if (kycResponse != null || kycResponse.getCustomerDetails() != null
        || userDetailsResponse != null || userDetailsResponse.getUserInformation() != null) {
      if (!valueMap.containsKey(StateMachineConstants.CUST_ID)
          || !valueMap.containsKey(StateMachineConstants.ACCOUNT_NUMBER)
          || !valueMap.containsKey(StateMachineConstants.IDC_CARD_ALIAS)) {
        throw new StateMachineException("custId or Account number or idc card alias not provided");
      }
      List<InstaAddress> addressList = null;
      if (kycResponse.getAddressDetails() != null) {
        InstaAddress addressDetails = new InstaAddress();
        addressDetails.setAddressType(1);
        addressDetails.setAddressLine1(AppUtils.validateLength(kycResponse.getAddressDetails().getAddressLine1(),FISParameterLengthLimit.IDC_MAX_ADDRESS_LINE1_LENGTH));
        addressDetails.setAddressLine2(AppUtils.validateLength(kycResponse.getAddressDetails().getAddressLine2(),FISParameterLengthLimit.IDC_MAX_ADDRESS_LINE2_LENGTH));
        addressDetails.setAddressLine3(".");//kyc address line 3 is not exist
        addressDetails.setCity(AppUtils.validateLength(kycResponse.getAddressDetails().getCity(),
            FISParameterLengthLimit.MAX_FF_CITY_LENGTH));
        addressDetails
            .setPostalCode(AppUtils.validateLength(kycResponse.getAddressDetails().getPostalCode(),
                FISParameterLengthLimit.MAX_FF_POSTAL_CODE_LENGTH));
        addressDetails.setState(AppUtils.validateLength(kycResponse.getAddressDetails().getState(),
            FISParameterLengthLimit.MAX_FF_STATE_LENGTH));
        addressList = Collections.singletonList(addressDetails);
      }
      String custId = (String) valueMap.get(StateMachineConstants.CUST_ID);
      String typeCode = (String) valueMap.get(StateMachineConstants.CARD_ACCOUNT_TYPE_CODE);
//      oauthProxy.isCustIdAllowed(custId);
      LOGGER.info("IDC issue request for custId {}", custId);
      String dob = DateUtil.getCustomDateFormat(kycResponse.getCustomerDetails().getDob(),
          Constants.DATE_FORMAT.KYC, Constants.DATE_FORMAT.FIS);
      dob = AppUtils.validateLength(dob, FISParameterLengthLimit.MAX_FF_DOB_LENGTH);
      if (dob.equalsIgnoreCase(".")) {
        dob = "1999-12-12"; // rare case scenario, if we get DOB empty/null by chance from kyc
      }
      CustomerDetails customerDetails =
          setCIFStandardName(kycResponse, FISParameterLengthLimit.MAX_CUST_NAME_LENGTH);
      InstaIssueRequest instaIssueRequest = new InstaIssueRequest(custId,
          customerDetails.getFirstName(), customerDetails.getLastName(), dob,
          AppUtils.validateLength(kycResponse.getCustomerDetails().getGender(),
              FISParameterLengthLimit.MAX_FF_GENDER_LENGTH),
          1, userDetailsResponse.getUserInformation().getPhone(),
          (String) valueMap.get(StateMachineConstants.ACCOUNT_NUMBER), addressList,typeCode);
    //  AppUtils.validateObjectAndThrowException(instaIssueRequest);
      String processId = MDC.get(Constants.REQUEST_ID);
      ApiResponse apiResponse = null;
      try {
        StringBuilder sb = new StringBuilder(hostUrl);
        String url = sb.append(ISSUE_IDC).append("/")
            .append(valueMap.get(StateMachineConstants.IDC_CARD_ALIAS)).append("/").append("issue")
            .toString();

        HttpHeaders httpHeaders = new HttpHeaders();
        httpHeaders.set(requestToken, processId);

        HttpEntity<InstaIssueRequest> httpEntity = new HttpEntity<>(instaIssueRequest, httpHeaders);
        LOGGER.info("Requesting IDC issue eis url {} ", url);
        long start = System.currentTimeMillis();
//        ResponseEntity<JsonNode> responseEntity =
//            restClientUtil.makePostRequest(url, httpEntity, JsonNode.class);
        ResponseEntity<JsonNode> responseEntity = triggerRequest(url, httpEntity, JsonNode.class, POST);
        long end = System.currentTimeMillis();
        if (responseEntity != null) {
          apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(),
              InstaCardReplacementResponse.class, FailureResponse.class);
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.IDC_CARD_ISSUE,
              end - start);
        }
      } catch (HystrixRuntimeException hre) {
        String response = fetchHystrixExceptionMessage(hre);
        LOGGER.error("error while IDC issuence for given for custId {} {} : {} ", custId, response,
            hre);
        apiResponse = AppUtils.hystrixExceptionApiResponse(hre);
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.IDC_CARD_ISSUE);
      } catch (HttpClientErrorException e) {
        apiResponse = new ApiResponse();
        AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.IDC_CARD_ISSUE);
      } catch (BadRequestException e) {
        LOGGER.error("error while IDC issuence for given for custId " + custId, e);
        apiResponse = new ApiResponse();
        AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.IDC_CARD_ISSUE);
      } catch (Exception e) {
        LOGGER.error("error while IDC issuence for given for custId " + custId, e);
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
            PSStatUtils.Entity.Instance.FIS.IDC_CARD_ISSUE);
      }
      if (apiResponse != null && apiResponse.isSuccess()) {
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.IDC_CARD_ISSUE);
      }
      return apiResponse;
    }
    throw new StateMachineException(
        "Either KYCResponse or KYC customerDetail or KYC addressDetail or userDetailsResponse or userInformation is null");
  }

  private <T> ResponseEntity<T> triggerRequest(String url, HttpEntity<?> httpEntity,
      Class<T> responseType, Constants.RequestType requestType)
      throws UnsupportedEncodingException {

    ResponseEntity<T> response;
    boolean isHystrixEnable = DynamicPropertyReader.getBooleanProperty(FIS_ENABLE_HYSTRIX, true);
    httpEntity = eisHelper.populateJwtToken(httpEntity);

    if (isHystrixEnable) {
      LOGGER.debug("Hystrix enabled {} fis request", requestType.toString());
      FisCommandWrapper.FisHystrixCommand fisHystrixCommand = fisCommandWrapper
          .createFisCommand(url, httpEntity, ResponseEntity.class, responseType, requestType);
      fisHystrixCommand.setRequestId(MDC.get(Constants.REQUEST_ID));
      response = (ResponseEntity<T>) fisHystrixCommand.execute();
    } else {
      LOGGER.debug("Hystrix disabled {} fis request", requestType.toString());
      switch (requestType) {
        case PUT:
          response = restClientUtil.makePutRequest(url, httpEntity, responseType);
          break;
        case POST:
          response = restClientUtil.makePostRequest(url, httpEntity, responseType);
          break;
        default:
          response = null;
          break;
      }
    }

    return response;
  }

  private JsonNode triggerGetApi(URI uri, HttpHeaders httpHeaders, String processId)
          throws Exception {
    boolean isHystrixEnable = DynamicPropertyReader.getBooleanProperty(FIS_ENABLE_HYSTRIX, true);
    httpHeaders = eisHelper.populateJwtToken(httpHeaders);

    JsonNode response;
    if (isHystrixEnable) {
      LOGGER.debug("Hystrix enabled GET fis request");
      FisCommandWrapper.FisHystrixCommand fisHystrixCommand = fisCommandWrapper
              .createGetCommand(uri, httpHeaders, JsonNode.class, JsonNode.class, processId);
      fisHystrixCommand.setRequestId(MDC.get(Constants.REQUEST_ID));
      response = (JsonNode) fisHystrixCommand.execute();
    } else {
      LOGGER.debug("Hystrix disabled GET fis request");
      response = restClientUtil.getApiResult(uri, httpHeaders, JsonNode.class, processId);
    }

    return response;
  }

  private String fetchHystrixExceptionMessage(HystrixRuntimeException hre) {
    String response;
    if (hre.getCause() instanceof HttpStatusCodeException) {
      response = ((HttpStatusCodeException) hre.getCause()).getResponseBodyAsString();
    } else if (hre.getCause() instanceof ResourceAccessException) {
      ResourceAccessException rae = (ResourceAccessException) hre.getCause();
      response = rae.getMessage();
    } else {
      response = hre.getMessage();
    }
    return response;
  }


  /** API to check IDC order status
   * @param invoiceNumber
   * @return
   */
  public ApiResponse idcCheckOrderStatus(String invoiceNumber) {
    validateFISApiSwitch(GET);
    LOGGER.debug("idcCheckOrderStatus request with invoiceNumber {}", invoiceNumber);
    String processId = MDC.get(Constants.REQUEST_ID);
    ApiResponse apiResponse = null;
    try {
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_IDC).append("/").append("invoices").append("/")
          .append(invoiceNumber).toString();

      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, processId);
      httpHeaders = eisHelper.populateJwtToken(httpHeaders);
      HttpEntity<JsonNode> httpEntity = new HttpEntity<>(null, httpHeaders);
      LOGGER.info("Requesting idcCheckOrderStatus eis url {} ", url);
      long start = System.currentTimeMillis();
      ResponseEntity<JsonNode> responseEntity =
          restClientUtil.makeGetRequest(url, httpEntity, JsonNode.class);
      long end = System.currentTimeMillis();
      if (responseEntity != null) {
        apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(), CardOrderStatusResponse.class,
            FailureResponse.class);
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.IDC_CHECK_ORDER_STATUS,
            end - start);
      }
    } catch (HttpClientErrorException | HttpServerErrorException e) {
      apiResponse = new ApiResponse();
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CHECK_ORDER_STATUS);
    } catch (BadRequestException e) {
      LOGGER.error("Error while idcCheckOrderStatus for invoice number {} " + invoiceNumber, e);
      apiResponse = new ApiResponse();
      AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CHECK_ORDER_STATUS);
    } catch (Exception e) {
      LOGGER.error("error while idcCheckOrderStatus for invoice number {} " + invoiceNumber, e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CHECK_ORDER_STATUS);
    }
    if (apiResponse != null && apiResponse.isSuccess()) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.IDC_CHECK_ORDER_STATUS);
    }
    return apiResponse;
  }


  /** API to check IDC order status
   * @param invoiceNumber
   * @return
   */
  public ApiResponse idcCancelOrder(String invoiceNumber) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("idcCancelOrder request with invoiceNumber {}", invoiceNumber);
    String processId = MDC.get(Constants.REQUEST_ID);
    ApiResponse apiResponse = null;
    try {
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_IDC).append("/").append("invoices").append("/")
          .append(invoiceNumber).toString();

      HttpHeaders httpHeaders = new HttpHeaders();
      Map<String,String> mapBody=new HashMap<>();
      mapBody.put("action", "CANCEL");
      httpHeaders.set(requestToken, processId);
      httpHeaders.set("Content-Type", "application/json");
      httpHeaders = eisHelper.populateJwtToken(httpHeaders);
      HttpEntity<String> httpEntity = new HttpEntity<>(AppUtils.toJson(mapBody), httpHeaders);
      LOGGER.info("Requesting idcCancelOrder eis url {} ", url);
      long start = System.currentTimeMillis();
      ResponseEntity<JsonNode> responseEntity =
          restClientUtil.makePutRequest(url, httpEntity, JsonNode.class);
      long end = System.currentTimeMillis();
      if (responseEntity != null) {
        apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(), CardOrderRequest.class,
            FailureResponse.class);
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.IDC_CANCEL_ORDER,
            end - start);
      }
    } catch (HttpClientErrorException | HttpServerErrorException e) {
      apiResponse = new ApiResponse();
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CANCEL_ORDER);
    } catch (BadRequestException e) {
      LOGGER.error("Error while idcCancelOrder for invoice number {}" + invoiceNumber, e);
      apiResponse = new ApiResponse();
      AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CANCEL_ORDER);
    } catch (Exception e) {
      LOGGER.error("error while idcCancelOrder for invoice number {} " + invoiceNumber, e);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.IDC_CANCEL_ORDER);
    }
    if (apiResponse != null && apiResponse.isSuccess()) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.IDC_CANCEL_ORDER);
    }
    return apiResponse;
  }



  public ApiResponse inquireVDCCifForCorporateUser(String userId, String accountNumber) {
    validateFISApiSwitch(GET);
    ApiResponse apiResponse = null;
    try {
      String txnId = MDC.get(Constants.REQUEST_ID);
      String url =
          hostUrl + CARD_ENDPOINT2 + "/" + userId + "/card" + "?accountNumber=" + accountNumber;
      long start = System.currentTimeMillis();
      LOGGER.info("Requesting eis url {} ", url);
      JsonNode jsonResponse = triggerGetApi(new URI(url), null, txnId);
      long end = System.currentTimeMillis();
      LOGGER.info("time taken in coroprate vdc creation : {}", end - start);
      PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS, (end - start));
      if (jsonResponse != null) {
        apiResponse = AppUtils.parseJsonResponse(jsonResponse, DebitCardDetailsResponse.class,
            FailureResponse.class);
      }
    } catch (HttpClientErrorException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getResponseBodyAsString());
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
    } catch (HttpServerErrorException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getResponseBodyAsString());
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
    } catch (BadRequestException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getMessage());
      AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
    } catch (Exception e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_DETAILS);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getMessage());
      AppUtils.handleBadRequestException(e.getMessage(), apiResponse);
    }
    return apiResponse;
  }


  public ApiResponse createVDCForICA(Map<String, Object> valueMap) {
    validateFISApiSwitch(POST);
    String processId = MDC.get(Constants.REQUEST_ID);
    ApiResponse apiResponse = null;
    try {
      IssueCardRequest issueCardRequest = createDCRequest(valueMap);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(CARD_ENDPOINT).toString();
      oauthProxy.isCustIdAllowed(issueCardRequest.getCustomerId());
      issueCardRequest.setCaIndiv(true);
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, processId);
      HttpEntity<IssueCardRequest> httpEntity = new HttpEntity<>(issueCardRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
      ResponseEntity<JsonNode> responseEntity =
          triggerRequest(url, httpEntity, JsonNode.class, POST);
      if (responseEntity != null) {
        apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(),
            IssueCardResponse.class, FailureResponse.class);
        long end = System.currentTimeMillis();
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD,
            end - start);
      }
    } catch (HystrixRuntimeException hre) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("error while creating corporate vdc issuence for given for custId {} {}  ",
          response, hre);
      apiResponse = AppUtils.hystrixExceptionApiResponse(hre);
    }
    catch (HttpClientErrorException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getResponseBodyAsString());
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
    } catch (HttpServerErrorException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getResponseBodyAsString());
    } catch (BadRequestException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getMessage());
    } catch (Exception e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_DEBIT_CARD);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getMessage());
    }
    return apiResponse;
  }

  public ApiResponse createCorporateMasterCard(KybResponse kybResponse,Map<String,Object> valueMap) {
    validateFISApiSwitch(POST);
    ApiResponse apiResponse = new ApiResponse();
    try {
      CorpCardCifCreationRequest issueCardRequest = createCorporateMasterCardRequest(kybResponse,valueMap);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(CORPORATE_MASTER_CARD_ENDPOINT).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      String processId = MDC.get(Constants.REQUEST_ID);
      httpHeaders.set(requestToken, (StringUtils.isNotBlank(processId)) ? processId
          : (String) valueMap.get(StateMachineConstants.CORPORATE_ACCOUNT_ID));
      HttpEntity<CorpCardCifCreationRequest> httpEntity = new HttpEntity<>(issueCardRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
      ResponseEntity<JsonNode> responseEntity =
          triggerRequest(url, httpEntity, JsonNode.class, POST);
      if (responseEntity != null) {
        apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(), IssueCardResponse.class,
            FailureResponse.class);
        long end = System.currentTimeMillis();
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.CREATE_CORP_CIF,
            end - start);
      }
    } catch (HystrixRuntimeException hre) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_CIF);
      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("error while CaCorpMasterCard issuence for given for custId {} {}  ", response,
          hre);
      apiResponse = AppUtils.hystrixExceptionApiResponse(hre);
    }
    catch (HttpClientErrorException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_CIF);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getResponseBodyAsString());
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
    } catch (HttpServerErrorException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_CIF);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getResponseBodyAsString());
    } catch (BadRequestException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_CIF);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getMessage());
    } catch (Exception e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_CIF);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getMessage());
    }
    return apiResponse;
  }

  //Set the first Name and lastName based on Company name length format
  public CustomerDetails setCompanyName(String name, int length) {
    CustomerDetails customerDetails = new CustomerDetails();
    if (name == null || name.isEmpty() || name.trim().isEmpty()) {
      customerDetails.setFirstName(".");
      customerDetails.setLastName("");
      return customerDetails;
    }
    name = name.trim().replaceAll(" +", " ");
    String fName = "", lName = "";
    if (name.length() < length) {
      fName = name.toUpperCase();
    } else {
      String str[] = name.split(" ");
      if (str.length == 1) {
        fName = getCustomName(str[0], length);
      } else {
        fName = getCustomName(str[0], length);
        lName = getCustomName(name.substring(str[0].length() + 1), length);
      }
    }
    customerDetails.setFirstName(fName);
    customerDetails.setLastName(lName);

    return customerDetails;
  }

  private CorpCardCifCreationRequest createCorporateMasterCardRequest(KybResponse kybResponse,
      Map<String, Object> valueMap) {
    CorpCardCifCreationRequest issueCardRequest = new CorpCardCifCreationRequest();
    UserDetailsResponse userDetailsResponse =
        (UserDetailsResponse) valueMap.get(StateMachineConstants.OAUTH_RESPONSE);
    issueCardRequest
        .setAccountNumber((String) valueMap.get(StateMachineConstants.CORPORATE_ACCOUNT_NO));
    KybCompanyDetails kybCompanyDetails = kybResponse.getCompany().get(0);
    issueCardRequest.setCustomerId(kybCompanyDetails.getBusinessId().toUpperCase());
    KybAddressDetails kybAddressDetails =
        kybCompanyDetails.getBusinessResources().getAddress().get(0);
    CustomerDetails customerDetails=setCompanyName(kybCompanyDetails.getName(),FISParameterLengthLimit.MAX_CUST_NAME_LENGTH);
    issueCardRequest.setCountry("IN");
    issueCardRequest.setFirstName(customerDetails.getFirstName());
    issueCardRequest.setLastName(customerDetails.getLastName());
    issueCardRequest.setAddressLine1(kybAddressDetails.getLine1());
    issueCardRequest.setAddressLine2(kybAddressDetails.getLine2());
/*    issueCardRequest.setDob(DateUtil.getCustomDateFormat(kybCompanyDetails.getDateOfIncorporation(),
        Constants.EXTERNAL_PARAMS.DATE_FORMAT.KYB, Constants.EXTERNAL_PARAMS.DATE_FORMAT.FIS));
*/    issueCardRequest.setCity(kybAddressDetails.getDistrict());
    issueCardRequest.setPostalCode(kybAddressDetails.getpostalCode());
    issueCardRequest.setKycStatus("1");
    issueCardRequest.setPhone(userDetailsResponse.getUserInformation().getPhone());
    issueCardRequest.setBrnCode((String) valueMap.get(StateMachineConstants.SOL_ID));
    issueCardRequest.setTypeCode((String) valueMap.get(StateMachineConstants.CARD_ACCOUNT_TYPE_CODE));
    return issueCardRequest;
  }

  public BaseResponse updateCIF(String custId, String cifUpdateJsonRequest) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("Enter:Update CIF api for custId {}", custId);
    String requestId = MDC.get(Constants.REQUEST_ID);
    try {
      CustomPDCResponse<?> cifUpdateResponse;
      StringBuilder cifRequest = new StringBuilder(cifUpdateJsonRequest);
      int startIndex = cifUpdateJsonRequest.indexOf("pan");
      cifRequest.replace(startIndex + 5, startIndex + 22, "\"xxxxx");
      LOGGER.info("cifUpdateJsonRequest for custId {} is {} : ", custId, cifRequest);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(ISSUE_PDC).append("/").append(custId).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set("Request-Token", requestId);
      httpHeaders.set("Accept", "application/json");
      httpHeaders.setContentType(MediaType.APPLICATION_JSON);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
      ResponseEntity<String> responseEntity =
          sendHttpQrRequest(httpHeaders, url, cifUpdateJsonRequest);
      long end = System.currentTimeMillis();
      LOGGER.info("cifUpdateResponseEntity for custId {} is {} : ", custId, responseEntity);

      if (responseEntity != null && responseEntity.getStatusCode() == HttpStatus.OK) {
        cifUpdateResponse = AppUtils.fromJson(responseEntity.getBody(), CustomPDCResponse.class);

        if (null != cifUpdateResponse && CustomResponse.Status.SUCCESS.name()
            .equalsIgnoreCase(cifUpdateResponse.getStatus())) {
          LOGGER.info("Got success response while Updating CIF for custId {}", custId);
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.CIF_UPDATE, end - start);
          return cifUpdateResponse;

        } else if (null != cifUpdateResponse && !CustomResponse.Status.SUCCESS.name()
            .equalsIgnoreCase(cifUpdateResponse.getStatus())) {
          LOGGER.info("Got failure response from EIS while Updating CIF for custId {}", custId);
          if (cifUpdateResponse.getResponse() != null) {
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CIF_UPDATE);
            return AppUtils.convertValue(cifUpdateResponse.getResponse(), FailureResponse.class);
          }
        }
      }
      LOGGER.error("responseEntity for updateCIF is null or response code is not 200 for custid {}",
          custId);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.CIF_UPDATE);
      LOGGER.debug("Exiting Update CIF api for custid {}", custId);
      throw new InternalServerException(MessageConstants.UPDATE_CIF_ERR.getMessage());
    } catch (Exception e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CIF_UPDATE);
      LOGGER.error("error while UpdateCIF Status for custId {} due to {} ", custId, e);
      throw e;
    }
  }

  public ApiResponse createCorporateCustomerVDC(Map<String, Object> valueMap) {
    validateFISApiSwitch(POST);
    ApiResponse apiResponse = new ApiResponse();
    try {
      CorpCardCustomerOnBoardingRequest issueCardRequest =
          createCorporateCustomerVDCRequest(valueMap);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url = sb.append(CORPORATE_CARD_FOR_CA).toString();
      url = url.replace("MASTER_CARD_NO",
          (String) valueMap.get(StateMachineConstants.CORP_MASTER_CARD_ALIAS));
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken,
          (String) valueMap.get(StateMachineConstants.CORPORATE_ACCOUNT_ID));
      HttpEntity<CorpCardCustomerOnBoardingRequest> httpEntity =
          new HttpEntity<>(issueCardRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      long start = System.currentTimeMillis();
      ResponseEntity<JsonNode> responseEntity =
          triggerRequest(url, httpEntity, JsonNode.class, POST);
      if (responseEntity != null) {
        apiResponse = AppUtils.parseJsonResponse(responseEntity.getBody(), IssueCardResponse.class,
            FailureResponse.class);
        long end = System.currentTimeMillis();
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.CREATE_CORP_MASTER_CARD,
            end - start);
      }
    } catch (HystrixRuntimeException hre) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_MASTER_CARD);
      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("error while CaCorpMasterCard issuence for given for custId {} {}  ", response,
          hre);
      apiResponse = AppUtils.hystrixExceptionApiResponse(hre);
    } catch (HttpClientErrorException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_MASTER_CARD);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getResponseBodyAsString());
      AppUtils.handleHttpClientErrorException(e.getResponseBodyAsString(), apiResponse);
    } catch (HttpServerErrorException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_MASTER_CARD);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getResponseBodyAsString());
    } catch (BadRequestException e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_MASTER_CARD);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getMessage());
    } catch (Exception e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CREATE_CORP_MASTER_CARD);
      apiResponse = new ApiResponse();
      apiResponse.setSuccess(false);
      apiResponse.setMessage(e.getMessage());
    }
    return apiResponse;
  }

  private CorpCardCustomerOnBoardingRequest createCorporateCustomerVDCRequest(
      Map<String, Object> valueMap) {
    CorpCardCustomerOnBoardingRequest issueCardRequest = new CorpCardCustomerOnBoardingRequest();
    KycResponse kycResponse = (KycResponse) valueMap.get(StateMachineConstants.KYC_RESPONSE);
    UserDetailsResponse userDetailsResponse =
        (UserDetailsResponse) valueMap.get(StateMachineConstants.OAUTH_RESPONSE);
    /*
     * issueCardRequest .setAccountNumber((String)
     * valueMap.get(StateMachineConstants.CORPORATE_ACCOUNT_NO));
     */
    issueCardRequest.setAddressLine1(kycResponse.getAddressDetails().getAddressLine1());
    issueCardRequest.setAddressLine2(kycResponse.getAddressDetails().getAddressLine2());
    issueCardRequest.setCity(kycResponse.getAddressDetails().getCity());
    issueCardRequest.setCountry("IND");
    issueCardRequest.setCustomerId(userDetailsResponse.getUserId());
    issueCardRequest.setDob(DateUtil.getCustomDateFormat(kycResponse.getCustomerDetails().getDob(),
        Constants.EXTERNAL_PARAMS.DATE_FORMAT.KYC, Constants.EXTERNAL_PARAMS.DATE_FORMAT.FIS));

    CustomerDetails customerDetails =
        setCIFStandardName(kycResponse, FISParameterLengthLimit.MAX_CUST_NAME_LENGTH);

    String firstNameInUpperCase = customerDetails.getFirstName() == null ? null
        : customerDetails.getFirstName().toUpperCase();
    String lastNameInUpperCase =
        customerDetails.getLastName() == null ? null : customerDetails.getLastName().toUpperCase();

    issueCardRequest.setFirstName(firstNameInUpperCase);
    issueCardRequest.setKycStatus("1");
    issueCardRequest.setLastName(lastNameInUpperCase);
    issueCardRequest.setPhone(userDetailsResponse.getUserInformation().getPhone());
    issueCardRequest.setPostalCode(kycResponse.getAddressDetails().getPostalCode());
    issueCardRequest.setState(kycResponse.getAddressDetails().getState());
    issueCardRequest.setBrnCode((String) valueMap.get(StateMachineConstants.SOL_ID));
    Set<String> messages = AppUtils.validate(issueCardRequest);
    if (AppUtils.validate(issueCardRequest) != null) {
      throw new BadRequestException(messages.toString().replace("[", "").replace("]", ""));
    } ;
    validateEmboss(issueCardRequest.getFirstName(), issueCardRequest.getLastName());
    return issueCardRequest;
  }

  public boolean createPhysicalDebitCard(IssuePDCRequest issuePDCardRequest) {
    validateFISApiSwitch(POST);
    boolean isCreated = false;
    String processId = MDC.get(Constants.REQUEST_ID);
    try {
      String url = hostUrl + ISSUE_PDC;
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, processId);
      HttpEntity<IssuePDCRequest> httpEntity = new HttpEntity<>(issuePDCardRequest, httpHeaders);
      LOGGER.info(
          "Creating physical debit card for custId {} awb {} orderId {} courier {} cardAlias {} url {}",
          issuePDCardRequest.getCustomerId(), issuePDCardRequest.getAwbNumber(),
          issuePDCardRequest.getOrderId(), issuePDCardRequest.getCourierVendor(),
          issuePDCardRequest.getCardAlias(), url);

      long start = System.currentTimeMillis();
      ResponseEntity<JsonNode> responseEntity =
          triggerRequest(url, httpEntity, JsonNode.class, POST);
      long end = System.currentTimeMillis();
      if (responseEntity == null || responseEntity.getBody() == null) {
        LOGGER.error("error while creating PDC for custId {} response {}",
            issuePDCardRequest.getCustomerId(), responseEntity);
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
      } else {
        JsonNode jsonResponse = responseEntity.getBody();
        JsonNode responseStatus = jsonResponse.get("status");
        JsonNode response = jsonResponse.get("response");
        if (responseStatus == null || response == null) {
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
          LOGGER.error("error while creating PDC for custId {}, response {}",
              issuePDCardRequest.getCustomerId(), responseEntity);
          throw new DownStreamServerException("FIS is returning null repose or status");
        }
        if (CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(responseStatus.asText())) {
          LOGGER.info("Got success response while creating Physical Debit Card [Cust-Id: {}]",
              issuePDCardRequest.getCustomerId());
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD, end - start);
          isCreated = true;
        } else if (CustomResponse.Status.FAILURE.name().equalsIgnoreCase(responseStatus.asText())) {
          FailureResponse failureResponse = AppUtils.getErrorFromEis(response);
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
          LOGGER.error("error while creating PDC for custId {}, response {}",
              issuePDCardRequest.getCustomerId(), responseEntity);
          if ("Virtual to Physical card request Already initiated"
              .equalsIgnoreCase(failureResponse.getMessage())) {
            isCreated = true;
          } else {
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
            throw new InternalServerException(failureResponse.getMessage());
          }
        } else {
          LOGGER.error("error while creating PDC for custId {}, response {}",
              issuePDCardRequest.getCustomerId(), responseEntity);
          PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
              PSStatUtils.Entity.Instance.FIS.CREATE_PHYSICAL_DEBIT_CARD);
        }
      }
    } catch (HystrixRuntimeException hre) {
      String response = fetchHystrixExceptionMessage(hre);
      LOGGER.error("error while creating physical debitcard  : " + response, hre);
    } catch (Exception e) {
      LOGGER.error("error while creating physical debitcard  : ", e);
    }
    return isCreated;
  }

  /**
   * EIS API for Card Reissuance
   *
   * @param pan
   * @param custId
   * @return
   */
  public BaseResponse corporateCardReIssuance(String pan, String masterCardAliasNumber,
      String custId) {
    validateFISApiSwitch(PUT);
    LOGGER.debug("Enter:cardReissuance for custId {} ", custId);
    String requestId = MDC.get(Constants.REQUEST_ID);
    ResponseEntity<String> responseEntity = null;
    try {
      // oauthProxy.isCustIdAllowed(custId);
      CustomPDCResponse<?> cardReissuanceResponse;
      String cardReIssueJsonRequest = generateCorporateCardReissuanceRequest(masterCardAliasNumber);
      StringBuilder sb = new StringBuilder(hostUrl);
      String url =
          sb.append(CORPORATE_CARD_RE_ISSUE_VERSION).append(CORPORATE_CARD_RE_ISSUE_API).toString();
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set("Request-Token", requestId);
      httpHeaders.set("Accept", "application/json");
      httpHeaders.set("Card-Number", pan);
      httpHeaders.setContentType(MediaType.APPLICATION_JSON);
      LOGGER.info("Requesting eis url  for custId {} is {} ", custId, url);
      long start = System.currentTimeMillis();
      responseEntity = sendHttpQrRequest(httpHeaders, url, cardReIssueJsonRequest);
      long end = System.currentTimeMillis();

      if (responseEntity != null && responseEntity.getStatusCode() == HttpStatus.OK) {
        cardReissuanceResponse =
            AppUtils.fromJson(responseEntity.getBody(), CustomPDCResponse.class);

        if (null != cardReissuanceResponse && CustomResponse.Status.SUCCESS.name()
            .equalsIgnoreCase(cardReissuanceResponse.getStatus())) {
          LOGGER.info("Got success response for card Reissuance for custID: {}", custId);
          PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
              PSStatUtils.Entity.Instance.FIS.CARD_REISSUANCE, end - start);
          IssDirectiveResponse response = AppUtils
              .convertValue(cardReissuanceResponse.getResponse(), IssDirectiveResponse.class);
          if (response.getCardDetResponse() != null) {
            LOGGER.info("received success response from FIS for card reissaunce of cardAlias: "
                + response.getCardDetResponse().getAlias());
          }
          return response;

        } else if (null != cardReissuanceResponse && !CustomResponse.Status.SUCCESS.name()
            .equalsIgnoreCase(cardReissuanceResponse.getStatus())) {
          LOGGER.info("Got failure response from EIS for card Reissuance for custID: {} ", custId);
          if (cardReissuanceResponse.getResponse() != null) {
            PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
                PSStatUtils.Entity.Instance.FIS.CARD_REISSUANCE);
            return AppUtils.convertValue(cardReissuanceResponse.getResponse(),
                FailureResponse.class);
          }
        }
      }
      LOGGER.error("responseEntity for updateCIF is null or response code is not 200 for custid {}",
          custId);
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.CARD_REISSUANCE);
      LOGGER.debug("Exiting card Reissuance api for custid {}", custId);
      throw new InternalServerException(MessageConstants.CARD_REISSUED_FAILURE.getMessage());
    } catch (Exception e) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.CARD_REISSUANCE);
      LOGGER.error("error while Reissuance Status for custId {} due to {} ", custId, e);
      throw e;
    }
  }

  /**
   *
   * @param masterCorpCardAlias
   * @return
   */
  private String generateCorporateCardReissuanceRequest(String masterCorpCardAlias) {
    ReIssuanceCorpCardRequest reIssuanceRequest = new ReIssuanceCorpCardRequest();
    reIssuanceRequest.setCardProduct("PYOV");
    reIssuanceRequest.setMasterCorpCardAlias(masterCorpCardAlias);
    return AppUtils.toJson(reIssuanceRequest);
  }

  private void validateFISApiSwitch(Constants.RequestType requestType) {
    boolean isApiMaintenanceActive = PropertiesContext.getInstance()
        .getApplicationProperty(Constants.FIS_PROPERTY.FIS_MAINTENANCE_FLAG, "false")
        .equalsIgnoreCase("true");

    boolean isGetApiSwitchOn = PropertiesContext.getInstance()
        .getApplicationProperty(Constants.FIS_PROPERTY.FIS_GET_API_ENABLED_DURING_MAINTENANCE,
            "false")
        .equalsIgnoreCase("true");

    boolean isGetRequest = requestType.name().equalsIgnoreCase(Constants.RequestType.GET.name());
    boolean isFisApiSwitchActive = !isApiMaintenanceActive || (isGetRequest && isGetApiSwitchOn);

    if (!isFisApiSwitchActive) {
      String errorMsg = getFISSwithOffErrorMsg();
      LOGGER.error("FISApiSwitch is off, throwing Downstream with msg " + errorMsg);
      throw new DownStreamServerException(errorMsg);
    }
  }

  private String getFISSwithOffErrorMsg() {
    String errorMsg = PropertiesContext.getInstance()
        .getApplicationProperty(Constants.FIS_PROPERTY.FIS_MAINTENANCE_ERROR_MSG, null);
    errorMsg = StringUtils.isNotBlank(errorMsg) ? errorMsg
        : MessageConstants.FIS_MAINTENANCE_ERROR.getMessage();
    return errorMsg;
  }

  public BaseResponse cardUpgrade(String custId, String cardProduct) {
 //TODO: cardUpgrade
    return null;
  }

  public String getCardNumber(String cardAliasNumber) {
    if (cardAliasNumber == null) {
      LOGGER.error("Input cardAliasNumber is null");
      throw new BadRequestException("Input cardAliasNumber is null");
    }
    BaseResponse cardDetails = getCardDetails(cardAliasNumber);
    return getCardNumber(cardDetails);
  }

  public ApiResponse setCardLimit(CardLimitUpdateRequest cardLimitUpdateRequest) {
    LOGGER.info("set card limit for <cardAlias> : {}", cardLimitUpdateRequest.getCardNumber());
    validateFISApiSwitch(POST);
    fisApisMaintenanceHandling();
    String requestId = MDC.get(Constants.REQUEST_ID);
    try {
      String url = hostUrl + CARD_LIMIT_UPDATE;
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, requestId);
      HttpEntity<CardLimitUpdateRequest> httpEntity =
          new HttpEntity<>(cardLimitUpdateRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      Instant start = Instant.now();
      ResponseEntity<JsonNode> responseEntity =
          triggerRequest(url, httpEntity, JsonNode.class, POST);
      Instant finish = Instant.now();
      if (responseEntity == null || responseEntity.getBody() == null) {
        LOGGER.error("Null response received from FIS for <cardAlias> : {}", cardLimitUpdateRequest.getCardNumber());
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.SET_CARD_TRANSACTION_LIMIT);
        throw new DownStreamServerException(
            "We are currently facing some issue. Request you to please try again later.");
      }
      JsonNode responseBody = responseEntity.getBody();
      JsonNode status = responseBody.get("status");
      if (status != null
          && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(status.asText())) {
        JsonNode response = responseBody.get("response");
        LOGGER.info(
            "card transaction limit successfuly updated in FIS for <cardAlias>: {} and the response is : {}",
            cardLimitUpdateRequest.getCardNumber(), response);
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.SET_CARD_TRANSACTION_LIMIT,
            Duration.between(start, finish).toMillis());
        return objectMapper.convertValue(response, ApiResponse.class);
      }
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.SET_CARD_TRANSACTION_LIMIT);
      JsonNode response = responseBody.get("response");
      LOGGER.error(
          "Unable to update card transaction limit for <cardAlias> : {} and the response is : {}",
          cardLimitUpdateRequest.getCardNumber(), response);
      throw new DownStreamServerException(
          "We are currently facing some issue. Request you to please try again later.");
    } catch (Exception ex) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.SET_CARD_TRANSACTION_LIMIT);
      LOGGER.error("Exception while updating card transaction limit : ", ex);
      throw new DownStreamServerException(
          "We are currently facing some issue. Request you to please try again later");
    }
  }

  public ApiResponse updateChannelAccessSettings(
      ChannelAccessUpdateRequest channelAccessUpdateRequest) {
    LOGGER.info("update channel access for <cardAlias> : {}",
        channelAccessUpdateRequest.getCardNumber());
    validateFISApiSwitch(POST);
    fisApisMaintenanceHandling();
    String requestId = MDC.get(Constants.REQUEST_ID);
    try {
      String url = hostUrl + CARD_CHANNEL_ACCESS_UPDATE;
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, requestId);
      HttpEntity<ChannelAccessUpdateRequest> httpEntity =
          new HttpEntity<>(channelAccessUpdateRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      Instant start = Instant.now();
      ResponseEntity<JsonNode> responseEntity =
          triggerRequest(url, httpEntity, JsonNode.class, POST);
      Instant finish = Instant.now();
      if (responseEntity == null || responseEntity.getBody() == null) {
        LOGGER.error("Null response received from FIS for <cardAlias> : {}", channelAccessUpdateRequest.getCardNumber());
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.UPDATE_CHANNEL_ACCESS);
        throw new DownStreamServerException(
            "We are currently facing some issue. Request you to please try again later.");
      }
      JsonNode responseBody = responseEntity.getBody();
      JsonNode status = responseBody.get("status");
      if (status != null
          && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(status.asText())) {
        JsonNode response = responseBody.get("response");
        LOGGER.info(
            "channel access successfuly updated at FIS for <cardAlias>: {} and the response is : {}",
            channelAccessUpdateRequest.getCardNumber(), response);
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.UPDATE_CHANNEL_ACCESS,
            Duration.between(start, finish).toMillis());
        return objectMapper.convertValue(response, ApiResponse.class);
      }
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.UPDATE_CHANNEL_ACCESS);
      JsonNode response = responseBody.get("response");
      LOGGER.error("Unable to update channel access for <cardAlias> : {} and the response is : {}",
          channelAccessUpdateRequest.getCardNumber(), response);
      throw new DownStreamServerException(
          "We are currently facing some issue. Request you to please try again later.");
    } catch (Exception ex) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.UPDATE_CHANNEL_ACCESS);
      LOGGER.error("Exception while updating channel access : ", ex);
      throw new DownStreamServerException(
          "We are currently facing some issue. Request you to please try again later.");
    }
  }

  public ApiResponse setCardLimitAndChannelAccessUpdate(
      LimitAndChannelUpdateRequest limitAndChannelUpdateRequest) {
    LOGGER.info("set card limit and update channel access for <cardAlias>:{}",
        limitAndChannelUpdateRequest.getCardNumber());
    validateFISApiSwitch(POST);
    fisApisMaintenanceHandling();
    String requestId = MDC.get(Constants.REQUEST_ID);
    try {
      String url = hostUrl + CARD_LIMIT_AND_CHANNEL_ACCESS_UPDATE;
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, requestId);
      HttpEntity<LimitAndChannelUpdateRequest> httpEntity =
          new HttpEntity<>(limitAndChannelUpdateRequest, httpHeaders);
      LOGGER.info("Requesting eis url {} ", url);
      Instant start = Instant.now();
      ResponseEntity<JsonNode> responseEntity =
          triggerRequest(url, httpEntity, JsonNode.class, POST);
      Instant finish = Instant.now();
      if (responseEntity == null || responseEntity.getBody() == null) {
        LOGGER.error("Null response received from FIS for <CardAlias> : {}", limitAndChannelUpdateRequest.getCardNumber());
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.SET_CARD_TRANSACTION_LIMIT_AND_CHANEL_ACCESS_UPDATE);
        throw new DownStreamServerException(
            "We are currently facing some issue. Request you to please try again later.");
      }
      JsonNode responseBody = responseEntity.getBody();
      JsonNode status = responseBody.get("status");
      if (status != null
          && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(status.asText())) {
        JsonNode response = responseBody.get("response");
        LOGGER.info(
            "card transaction limit and channel access successfuly updated in FIS for <cardAlias>: {} and the response is : {}",
            limitAndChannelUpdateRequest.getCardNumber(), response);
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.SET_CARD_TRANSACTION_LIMIT_AND_CHANEL_ACCESS_UPDATE,
            Duration.between(start, finish).toMillis());
        return objectMapper.convertValue(response, ApiResponse.class);
      }
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.SET_CARD_TRANSACTION_LIMIT_AND_CHANEL_ACCESS_UPDATE);
      JsonNode response = responseBody.get("response");
      LOGGER.error(
          "Unable to update card transaction limit and channel access for <cardAlias> : {} and the response is : {}",
          limitAndChannelUpdateRequest.getCardNumber(), response);
      throw new DownStreamServerException(
          "We are currently facing some issue. Request you to please try again later.");
    } catch (Exception ex) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.SET_CARD_TRANSACTION_LIMIT_AND_CHANEL_ACCESS_UPDATE);
      LOGGER.error("Exception while updating card transaction limit and channel access : ", ex);
      throw new DownStreamServerException(
          "We are currently facing some issue. Request you to please try again later");
    }
  }

  public CardDetailsV3Response getV3CardDetails(String cardAlias) {
    LOGGER.info("Fetching card details from FIS for <cardAlias> : {}", cardAlias);
    validateFISApiSwitch(GET);
    String requestId = MDC.get(Constants.REQUEST_ID);
    try {
      HttpHeaders httpHeaders = new HttpHeaders();
      httpHeaders.set(requestToken, requestId);
      String cardNumber=getCardNumber(cardAlias);
      Map<String, String> uriVariables = new HashMap<>();
      uriVariables.put("alias", cardNumber);
      URI uri = UriComponentsBuilder.fromUriString(hostUrl).path(V3_CARD_DETAILS)
          .buildAndExpand(uriVariables).toUri();
      LOGGER.info("Requesting eis url {} ", uri);
      Instant start = Instant.now();
      JsonNode responseJsonNode = triggerGetApi(uri, httpHeaders, requestId);
      Instant finish = Instant.now();
      if (responseJsonNode == null) {
        LOGGER.info("Null response received from FIS for <cardAlias> : {}", cardAlias);
        PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
            PSStatUtils.Entity.Instance.FIS.GET_CARD_SETTING_DETAILS);
        throw new DownStreamServerException(
            "We are currently facing some issue. Request you to please try again later.");
      }
      JsonNode status = responseJsonNode.get("status");
      if (status != null
          && CustomResponse.Status.SUCCESS.name().equalsIgnoreCase(status.asText())) {
        JsonNode response = responseJsonNode.get("response");
        LOGGER.info(
            "Card details successfully fetched from FIS for <cardAlias> : {} and the response is : {}",
            cardAlias, response);
        PSStatUtils.recordStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.SUCCESS,
            PSStatUtils.Entity.Instance.FIS.GET_CARD_SETTING_DETAILS,
            Duration.between(start, finish).toMillis());
        return objectMapper.convertValue(response, CardDetailsV3Response.class);
      }
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.FAILURE,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_SETTING_DETAILS);
      JsonNode response = responseJsonNode.get("response");
      LOGGER.error("Unable to get card details for <cardAlias> : {} and the response is : {}",
          cardAlias, response);
      throw new DownStreamServerException(
          "We are currently facing some issue. Request you to please try again later.");

    } catch (Exception ex) {
      PSStatUtils.incrementStats(PSStatUtils.Entity.FIS, PSStatUtils.Entity.Action.EXCEPTION,
          PSStatUtils.Entity.Instance.FIS.GET_CARD_SETTING_DETAILS);
      LOGGER.error("Exception while getting card details : ", ex);
      throw new DownStreamServerException(
          "We are currently facing some issue. Request you to please try again later.");
    }
  }

  private void fisApisMaintenanceHandling() {
    boolean isFisApiMaintenanceFlagTrue = PropertiesContext.getInstance()
        .getApplicationProperty(FIS_PROPERTY.FIS_API_MAINTENANCE_FLAG, "false")
        .equalsIgnoreCase("true");
    if (isFisApiMaintenanceFlagTrue) {
      String errorMsg = getFISSwithOffErrorMsg();
      LOGGER.error("FISApiSwitch is off, throwing Downstream with msg " + errorMsg);
      throw new DownStreamServerException(errorMsg);
    }
  }
}
