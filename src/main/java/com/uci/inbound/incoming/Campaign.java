package com.uci.inbound.incoming;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.uci.adapter.cdac.CdacBulkSmsAdapter;
import com.uci.adapter.cdac.TrackDetails;
import com.uci.adapter.provider.factory.ProviderFactory;
import com.uci.dao.models.XMessageDAO;
import com.uci.dao.repository.XMessageRepository;
import com.uci.dao.utils.XMessageDAOUtils;
import com.uci.utils.BotService;
import com.uci.utils.bot.util.BotUtil;
import com.uci.utils.dto.Adapter;
import com.uci.utils.kafka.SimpleProducer;
import lombok.extern.slf4j.Slf4j;
import messagerosa.core.model.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.xml.bind.JAXBException;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.function.Consumer;

@Slf4j
@CrossOrigin
@RestController
@RequestMapping(value = "/campaign")
public class Campaign {
    @Value("${campaign}")
    private String campaign;

    @Autowired
    public SimpleProducer kafkaProducer;

    @Autowired
    private ProviderFactory factoryProvider;

    @Autowired
    private BotService botService;

    @Autowired
    private XMessageRepository xMsgRepo;

    @Value("${inboundProcessed}")
    String topicSuccess;

    @Value("${inbound-error}")
    String topicFailure;

    @RequestMapping(value = "/start", method = RequestMethod.GET)
    public ResponseEntity<String> startCampaign(@RequestParam("campaignId") String campaignId) throws JsonProcessingException, JAXBException {
        botService.getBotNodeFromId(campaignId).subscribe(data -> {
                    SenderReceiverInfo from = new SenderReceiverInfo().builder().userID("9876543210").deviceType(DeviceType.PHONE).build();
                    SenderReceiverInfo to = new SenderReceiverInfo().builder().userID("admin").build();
                    MessageId msgId = new MessageId().builder().channelMessageId(UUID.randomUUID().toString()).replyId("7597185708").build();
                    XMessagePayload payload = new XMessagePayload().builder().text(data.getStartingMessage() == null ? null : data.getStartingMessage()).build();

                    Adapter adapterDto = null;
                    if (data.getLogicIDs() != null && data.getLogicIDs().get(0) != null && data.getLogicIDs().get(0).getAdapter() != null) {
                        adapterDto = data.getLogicIDs().get(0).getAdapter();
                    }

//                    JsonNode adapter = BotUtil.getBotNodeAdapter(data);

//                    data.getLogicIDs().get(0)
                    log.info("adapter:" + adapterDto + ", node:" + data);
                    if (adapterDto.getProvider() != null && adapterDto.getProvider().equalsIgnoreCase("firebase")) {
                        from.setDeviceType(DeviceType.PHONE_FCM);
                    } else if (adapterDto.getProvider() != null && adapterDto.getProvider().equalsIgnoreCase("pwa")) {
                        from.setDeviceType(DeviceType.PHONE_PWA);
                    }

                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                    XMessage xmsg = new XMessage().builder()
                            .botId(UUID.fromString(data.getId() == null ? null : data.getId()))
                            .app(data.getName() == null ? null : data.getName())
                            .adapterId(BotUtil.getBotNodeAdapterId(data))
                            .sessionId(BotUtil.newConversationSessionId())
                            .ownerId(data.getOwnerID() == null ? null : data.getOwnerID())
                            .ownerOrgId(data.getOwnerOrgID() == null ? null : data.getOwnerOrgID())
                            .from(from)
                            .to(to)
                            .messageId(msgId)
                            .messageState(XMessage.MessageState.REPLIED)
                            .messageType(XMessage.MessageType.TEXT)
                            .payload(payload)
                            .providerURI(adapterDto.getProvider())
                            .channelURI(adapterDto.getChannel())
                            .timestamp(timestamp.getTime())
                            .tags(BotUtil.getBotNodeTags(data))
                            .build();

                    XMessageDAO currentMessageToBeInserted = XMessageDAOUtils.convertXMessageToDAO(xmsg);
                    xMsgRepo.insert(currentMessageToBeInserted)
                            .doOnError(genericError("Error in inserting current message"))
                            .subscribe(xMessageDAO -> {
                                sendEventToKafka(xmsg);
                            });
                }
        );
        return new ResponseEntity<>("Notification Sending", HttpStatus.OK);
    }

    private void sendEventToKafka(XMessage xmsg) {
        String xmessage = null;
        try {
            xmessage = xmsg.toXML();
        } catch (JAXBException e) {
            kafkaProducer.send(topicFailure, "Start request for bot.");
        }
        kafkaProducer.send(topicSuccess, xmessage);
    }

    private Consumer<Throwable> genericError(String s) {
        return c -> {
            log.error(s + "::" + c.getMessage());
        };
    }

    @RequestMapping(value = "/pause", method = RequestMethod.GET)
    public void pauseCampaign(@RequestParam("campaignId") String campaignId) throws JsonProcessingException, JAXBException {
        kafkaProducer.send(campaign, campaignId);
        return;
    }

    @RequestMapping(value = "/resume", method = RequestMethod.GET)
    public void resumeCampaign(@RequestParam("campaignId") String campaignId) throws JsonProcessingException, JAXBException {
        kafkaProducer.send(campaign, campaignId);
        return;
    }

    @RequestMapping(value = "/status/cdac/bulk", method = RequestMethod.GET)
    public TrackDetails getCampaignStatus(@RequestParam("campaignId") String campaignId) {
        CdacBulkSmsAdapter iprovider = (CdacBulkSmsAdapter) factoryProvider.getProvider("cdac", "SMS");
        try {
             iprovider.getLastTrackingReport(campaignId);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }
}