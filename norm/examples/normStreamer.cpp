
#include "normApi.h"

#include "protoCheck.h"
#include "protoSocket.h"

#include <stdio.h>       // for printf(), etc
#include <stdlib.h>      // for srand()
#include <string.h>      // for strrchr(), memset(), etc
#include <sys/time.h>    // for gettimeofday()
#include <arpa/inet.h>   // for htons()
#include <fcntl.h>       // for, well, fnctl()
#include <errno.h>       // obvious child
#include <assert.h>      // embarrassingly obvious

class NormStreamer
{
    public:
        NormStreamer();
        ~NormStreamer();
        
        // some day build these directly into NORM API
        enum CCMode {NORM_FIXED, NORM_CC, NORM_CCE, NORM_CCL};
        
        enum 
        {
            MSG_HEADER_SIZE = 2,    // Big Endian message length header size
            MSG_SIZE_MAX = 65535    // (including length header)  
        };  
            
        bool OpenNormSession(NormInstanceHandle instance, 
                             const char*        addr,
                             unsigned short     port,
                             NormNodeId         nodeId);
        void CloseNormSession();
        
        void SetNormCongestionControl(CCMode ccMode);
        void SetNormTxRate(double bitsPerSecond)
        {
            assert(NORM_SESSION_INVALID != norm_session);
            NormSetTxRate(norm_session, bitsPerSecond);
        }
        void SetNormMulticastInterface(const char* ifaceName)
        {
            assert(NORM_SESSION_INVALID != norm_session);
            NormSetMulticastInterface(norm_session, ifaceName);
        } 
        void SetNormMessageTrace(bool state)
        {
            assert(NORM_SESSION_INVALID != norm_session);
            NormSetMessageTrace(norm_session, state);
        } 
        void AddAckingNode(NormNodeId ackId)
        {
            assert(NORM_SESSION_INVALID != norm_session);
            NormAddAckingNode(norm_session, ackId);
            norm_acking = true;  // invoke ack-based flow control
        }
        
        bool Start(bool sender, bool receiver);
        void Stop()
            {is_running = false;}
        bool IsRunning() const
            {return is_running;}
        void HandleNormEvent(const NormEvent& event);
        
        // Sender methods
        int GetInputFile() const
            {return input_fd;}
        void SetInputReady() 
            {input_ready = true;}
        bool InputReady() const
            {return input_ready;}
        bool InputNeeded() const
            {return input_needed;}
        void ReadInput();
        bool TxPending() const
            {return (!input_needed && (input_index < input_msg_length));}
        bool TxReady() const
            {return (tx_ready && (!norm_acking || (tx_stream_buffer_count < tx_stream_buffer_max)));}
        void SendData();
        unsigned int WriteToStream(const char* buffer, unsigned int numBytes);
        void FlushStream(bool eom, NormFlushMode flushMode);
        
        // Receiver methods
        bool RxNeeded() const
            {return rx_needed;}
        bool RxReady() const
            {return rx_ready;}
        void RecvData();
        int GetOutputFile() const
            {return output_fd;}
        void SetOutputReady()
            {output_ready = true;}
        bool OutputReady() const
            {return output_ready;}
        bool OutputPending() const
            {return (!rx_needed && (output_index < output_msg_length));}
        void WriteOutput();
        
        void OmitHeader(bool state) 
            {omit_header = state;}
        
        // These can only be called post-OpenNormSession()
        void SetSilentReceiver(bool state)
            {NormSetSilentReceiver(norm_session, true);}
        void SetTxLoss(double txloss)
            {NormSetTxLoss(norm_session, txloss);}
            
    private:
        NormSessionHandle   norm_session;
        bool                is_multicast;
        bool                is_running;     
                                                     
        // State variables for reading input messages for transmission
        ProtoSocket         input_socket;    // optional UDP socket to "listen"
        FILE*               input_file;
        int                 input_fd;      // stdin by default  
        bool                input_ready;                       
        bool                input_needed;    
        char                input_buffer[MSG_SIZE_MAX]; 
        unsigned int        input_msg_length;
        unsigned int        input_index;
                                                
        NormObjectHandle    tx_stream;
        bool                tx_ready;
        UINT16              tx_segment_size;
        unsigned int        tx_stream_buffer_max;
        unsigned int        tx_stream_buffer_count;
        unsigned int        tx_stream_bytes_remain;
        bool                tx_watermark_pending;
        bool                norm_acking;
        NormFlushMode       flush_mode;  // TBD - allow for "none", "passive", "active" options
        
        // Receive stream and state variables for writing received messages to output
        NormObjectHandle    rx_stream;
        bool                rx_ready;
        bool                rx_needed;
        bool                msg_sync;
        ProtoSocket         output_socket;  // optional UDP socket for recv msg output
        FILE*               output_file;
        int                 output_fd;    // stdout by default
        bool                output_ready;
        char                output_buffer[MSG_SIZE_MAX];
        unsigned int        output_msg_length;
        unsigned int        output_index;
        
        // These are some options mainly for testing purposes
        bool                omit_header;  // if "true", receive message length header is _not_ written to output
        //bool                rx_silent;
        //double              tx_loss;
            
};  // end class NormStreamer

NormStreamer::NormStreamer()
 : norm_session(NORM_SESSION_INVALID), is_multicast(false), is_running(false),
   input_socket(ProtoSocket::UDP), input_file(stdin), input_fd(fileno(stdin)), input_ready(true), 
   input_needed(false), input_msg_length(0), input_index(0),
   tx_stream (NORM_OBJECT_INVALID), tx_ready(true),  tx_segment_size(0), 
   tx_stream_buffer_max(0), tx_stream_buffer_count(0), tx_stream_bytes_remain(0), 
   tx_watermark_pending(false), norm_acking(false), flush_mode(NORM_FLUSH_ACTIVE),
   rx_stream(NORM_OBJECT_INVALID), rx_ready(false), rx_needed(true), msg_sync(false),
   output_socket(ProtoSocket::UDP), output_file(stdout), output_fd(fileno(stdout)), output_ready(true), 
   output_msg_length(0), output_index(0), 
   omit_header(false)//, rx_silent(false), tx_loss(0.0)
{
}


NormStreamer::~NormStreamer()
{
}

bool NormStreamer::OpenNormSession(NormInstanceHandle instance, const char* addr, unsigned short port, NormNodeId nodeId)
{
    if (NormIsUnicastAddress(addr))
        is_multicast = false;
    else
        is_multicast = true;
    norm_session = NormCreateSession(instance, addr, port, nodeId);
    if (NORM_SESSION_INVALID == norm_session)
    {
        fprintf(stderr, "normStreamer error: unable to create NORM session\n");
        return false;
    }
    
    if (is_multicast)
    {
        NormSetRxPortReuse(norm_session, true);
        // TBD - make full loopback a command line option?
        NormSetMulticastLoopback(norm_session, true);
    }
    
    // Set some default parameters (maybe we should put parameter setting in Start())
    NormSetDefaultSyncPolicy(norm_session, NORM_SYNC_STREAM);
    
    NormSetDefaultUnicastNack(norm_session, true);
    
    NormSetTxRobustFactor(norm_session, 2);
    
    return true;
}  // end NormStreamer::OpenNormSession()

void NormStreamer::CloseNormSession()
{
    if (NORM_SESSION_INVALID == norm_session) return;
    NormDestroySession(norm_session);
    norm_session = NORM_SESSION_INVALID;
}  // end NormStreamer::CloseNormSession()

void NormStreamer::SetNormCongestionControl(CCMode ccMode)
{
    assert(NORM_SESSION_INVALID != norm_session);
    switch (ccMode)
    {
        case NORM_CC:  // default TCP-friendly congestion control
            NormSetEcnSupport(norm_session, false, false, false);
            break;
        case NORM_CCE: // "wireless-ready" ECN-only congestion control
            NormSetEcnSupport(norm_session, true, true);
            break;
        case NORM_CCL: // "loss tolerant", non-ECN congestion control
            NormSetEcnSupport(norm_session, false, false, true);
            break;
        case NORM_FIXED: // "fixed" constant data rate
            NormSetEcnSupport(norm_session, false, false, false);
            break;
    }
    if (NORM_FIXED != ccMode)
        NormSetCongestionControl(norm_session, true);
    else
        NormSetCongestionControl(norm_session, false);
}  // end NormStreamer::SetNormCongestionControl()

bool NormStreamer::Start(bool sender, bool receiver)
{
    // TBD - make the NORM parameters command-line accessible
    unsigned int bufferSize = 8*1024*1024;
    if (receiver)
    {
        if (!NormStartReceiver(norm_session, 2*bufferSize))
        {
            fprintf(stderr, "normStreamer error: unable to start NORM receiver\n");
            return false;
        }
        NormSetRxSocketBuffer(norm_session, 4*1024*1024);
    }
    if (sender)
    {
        NormSetGrttEstimate(norm_session, 0.001);
        unsigned int segmentSize = 1400;
        unsigned int blockSize = 64;
        unsigned int numParity = 8;
        if (norm_acking)
        {   
            // ack-based flow control enabled on command-line, 
            // so disable timer-based flow control
            NormSetFlowControl(norm_session, 0.0);
        }
        // Pick a random instance id for now
        struct timeval currentTime;
        gettimeofday(&currentTime, NULL);
        srand(currentTime.tv_usec);  // seed random number generator
        NormSessionId instanceId = (NormSessionId)rand();
        if (!NormStartSender(norm_session, instanceId, bufferSize, segmentSize, blockSize, numParity))
        {
            fprintf(stderr, "normStreamer error: unable to start NORM sender\n");
            if (receiver) NormStopReceiver(norm_session);
            return false;
        }
        NormSetTxSocketBuffer(norm_session, 4*1024*1024);
        if (NORM_OBJECT_INVALID == (tx_stream = NormStreamOpen(norm_session, bufferSize)))
        {
            fprintf(stderr, "normStreamer error: unable to open NORM tx stream\n");
            NormStopSender(norm_session);
            if (receiver) NormStopReceiver(norm_session);
            return false;
        }
        tx_segment_size = segmentSize;
        tx_stream_buffer_max = NormGetStreamBufferSegmentCount(bufferSize, segmentSize, blockSize);
        tx_stream_buffer_max -= blockSize;  // a little safety margin (perhaps not necessary)
        tx_stream_buffer_count = 0;
        tx_stream_bytes_remain = 0;
        tx_watermark_pending = false;
        tx_ready = true;
        input_needed = true;
        input_ready = true;
    }
    is_running = true;
    return true;
}  // end NormStreamer::Start();

void NormStreamer::ReadInput()
{
    // The loop count makes sure we don't spend too much time here
    // before going back to the main loop to handle NORM events, etc
    unsigned int loopCount = 0;
    while (input_needed && input_ready && (loopCount < 1000))
    {
        loopCount++;
        //if (100 == loopCount)
        //    fprintf(stderr, "normStreamer ReadInput() loop count max reached\n");
        unsigned int numBytes;
        if (input_index < MSG_HEADER_SIZE)
        {
            // Reading message length header for next message to send
            numBytes = MSG_HEADER_SIZE - input_index;
        }
        else
        {
            // Reading message body
            assert(input_index < input_msg_length);
            numBytes = input_msg_length - input_index;
        }
        /*size_t result = fread(input_buffer + input_index, 1, numBytes, input_file);
        if (result > 0)
        {
            input_index += result;
            if (MSG_HEADER_SIZE == input_index)
            {
                // We have now read the message size header
                // TBD - support other message header formats?
                // (for now, assume 2-byte message length header)
                uint16_t msgSize ;
                memcpy(&msgSize, input_buffer, MSG_HEADER_SIZE);
                msgSize = ntohs(msgSize);
                input_msg_length = msgSize;
            }
            else if (input_index == input_msg_length)
            {
                // Message input complete
                input_index = 0;  // reset index for transmission phase
                input_needed = false;
                if (TxReady()) SendData();
            }   
            else
            {
                // Still need more input
                // (wait for next input notification to read more)
                input_ready = false;
            }
        }
        else
        {
            if (feof(input_file))
            {
                // end-of-file reached, TBD - trigger final flushing and wrap-up
                fprintf(stderr, "normStreamer: input end-of-file detected ...\n");
                if (norm_acking)
                    NormSetWatermark(norm_session, tx_stream, true);
                NormStreamClose(tx_stream, true);
                input_needed = false;
            }
            else if (ferror(input_file))
            {
                switch (errno)
                {
                    case EINTR:
                        continue;  // interrupted, try again
                    case EAGAIN:
                        // input starved, wait for next notification
                        input_ready = false;
                        break;
                    default:
                        // TBD - handle this better
                        perror("normStreamer error reading input");
                        break;
                }
            }
            break;
        }*/
        ssize_t result = read(input_fd, input_buffer + input_index, numBytes);
        if (result > 0)
        {
            input_index += result;
            if (MSG_HEADER_SIZE == input_index)
            {
                // We have now read the message size header
                // TBD - support other message header formats?
                // (for now, assume 2-byte message length header)
                uint16_t msgSize ;
                memcpy(&msgSize, input_buffer, MSG_HEADER_SIZE);
                msgSize = ntohs(msgSize);
                input_msg_length = msgSize;
            }
            else if (input_index == input_msg_length)
            {
                // Message input complete
                input_index = 0;  // reset index for transmission phase
                input_needed = false;
                if (TxReady()) SendData();
            }   
            else
            {
                // Still need more input
                // (wait for next input notification to read more)
                input_ready = false;
            }
        }
        else if (0 == result)
        {
            // end-of-file reached, TBD - trigger final flushing and wrap-up
            fprintf(stderr, "normStreamer: input end-of-file detected ...\n");
            if (norm_acking)
                NormSetWatermark(norm_session, tx_stream, true);
            NormStreamClose(tx_stream, true);
            input_needed = false;
        }
        else
        {
            switch (errno)
            {
                case EINTR:
                    continue;  // interrupted, try again
                case EAGAIN:
                    // input starved, wait for next notification
                    input_ready = false;
                    break;
                default:
                    // TBD - handle this better
                    perror("normStreamer error reading input");
                    break;
            }
            break;
        }
    }  // end while (input_needed && input_ready)
}  // end NormStreamer::ReadInput()

void NormStreamer::SendData()
{
    while (TxReady() && !input_needed)
    {
        // Note WriteToStream() or FlushStream() will set "tx_ready" to 
        // false upon flow control thus negating TxReady() status
        assert(input_index < input_msg_length);
        input_index += WriteToStream(input_buffer + input_index, input_msg_length - input_index);
        if (input_index == input_msg_length)
        {
            // Complete message was sent, so flush
            // TBD - provide flush "none", "passive", and "active" options
            FlushStream(true, flush_mode);
            input_index = input_msg_length = 0;
            input_needed = true;
        }
    }  // end while (TxReady() && !input_needed)
}  // end NormStreamer::SendData()

unsigned int NormStreamer::WriteToStream(const char* buffer, unsigned int numBytes)
{
    unsigned int bytesWritten;
    if (norm_acking)
    {
        // This method uses NormStreamWrite(), but limits writes by explicit ACK-based flow control status
        if (tx_stream_buffer_count < tx_stream_buffer_max)
        {
            // 1) How many buffer bytes are available?
            unsigned int bytesAvailable = tx_segment_size * (tx_stream_buffer_max - tx_stream_buffer_count);
            bytesAvailable -= tx_stream_bytes_remain;  // unflushed segment portiomn
            if (numBytes <= bytesAvailable) 
            {
                unsigned int totalBytes = numBytes + tx_stream_bytes_remain;
                unsigned int numSegments = totalBytes / tx_segment_size;
                tx_stream_bytes_remain = totalBytes % tx_segment_size;
                tx_stream_buffer_count += numSegments;
            }
            else
            {
                numBytes = bytesAvailable;
                tx_stream_buffer_count = tx_stream_buffer_max;        
            }
            // 2) Write to the stream
            bytesWritten = NormStreamWrite(tx_stream, buffer, numBytes);
            //assert(bytesWritten == numBytes);  // this could fail if timer-based flow control is left enabled
            // 3) Check if we need to issue a watermark ACK request?
            if (!tx_watermark_pending && (tx_stream_buffer_count >= (tx_stream_buffer_max / 2)))
            {
                // Initiate flow control ACK request
                NormSetWatermark(norm_session, tx_stream);
                tx_watermark_pending = true;
            }
        }
        else
        {
            fprintf(stderr, "normStreamer: sender flow control limited\n");
            bytesWritten = 0;
        }
    }
    else
    {
        bytesWritten = NormStreamWrite(tx_stream, buffer, numBytes);
    }
    if (bytesWritten != numBytes) tx_ready = false;
    return bytesWritten;
}  // end NormStreamer::WriteToStream()

void NormStreamer::FlushStream(bool eom, NormFlushMode flushMode)
{ 
    if (norm_acking)
    {
        bool setWatermark = false;
        if (0 != tx_stream_bytes_remain)
        {
            // The flush will force the runt segment out, so we increment our buffer usage count
            // (and initiate flow control watermark ack request if buffer mid-point threshold exceeded
            tx_stream_buffer_count++;
            tx_stream_bytes_remain = 0;
            if (!tx_watermark_pending && (tx_stream_buffer_count >= (tx_stream_buffer_max >> 1)))
            {
                setWatermark = true;
                tx_watermark_pending = true;
            }
        }
        // The check for "tx_watermark_pending" here prevents a new watermark
        // ack request from being set until the pending flow control ack is 
        // received. This favors avoiding dead air time over saving "chattiness"
        if (setWatermark)
        {
            // Flush passive since watermark will invoke active request
            // (TBD - do non-acking nodes NACK to watermark when not ack target?)
            NormStreamFlush(tx_stream, eom, NORM_FLUSH_PASSIVE);
        }
        else if (tx_watermark_pending)
        {
            // Pre-existing pending flow control watermark ack request
            NormStreamFlush(tx_stream, eom, flushMode);
        }
        else
        {
            // Since we're acking, use active ack request in lieu of active flush
            NormStreamFlush(tx_stream, eom, NORM_FLUSH_PASSIVE);
            if (NORM_FLUSH_ACTIVE == flushMode)
                setWatermark = true;
        }
        if (setWatermark) NormSetWatermark(norm_session, tx_stream, true);
    }
    else
    {
        NormStreamFlush(tx_stream, eom, flushMode);
    }
}  // end NormStreamer::FlushStream()

void NormStreamer::RecvData()
{    
    // The loop count makes sure we don't spend too much time here
    // before going back to the main loop to handle NORM events, etc
    unsigned int loopCount = 0;
    // Reads data from rx_stream to available output_buffer
    while (rx_needed && rx_ready && (loopCount < 1000))
    {
        loopCount++;
        //if (100 == loopCount)
        //    fprintf(stderr, "normStreamer RecvData() loop count max reached.\n");
        // Make sure we have msg_sync (TBD - skip this for byte streaming)
        if (!msg_sync)
        {
            msg_sync = NormStreamSeekMsgStart(rx_stream);
            if (!msg_sync) 
            {
                rx_ready = false;
                return;  // wait for next NORM_RX_OBJECT_UPDATED to re-sync
            }
        }
        unsigned int bytesWanted;
        if (output_index < MSG_HEADER_SIZE)
        {
            // Receiving message header
            bytesWanted = MSG_HEADER_SIZE - output_index;
        }
        else
        {
            // Receiving message body
            assert(output_index < output_msg_length);
            bytesWanted = output_msg_length - output_index;
        }
        unsigned bytesRead = bytesWanted;
        if (!NormStreamRead(rx_stream, output_buffer + output_index, &bytesRead))
        {
            // Stream broken (should _not_ happen if norm_acking flow control)
            fprintf(stderr, "normStreamer error: broken stream detected, re-syncing ...\n");
            msg_sync = false;
            output_index = output_msg_length = 0;
            continue;
        }
        output_index += bytesRead;
        if (bytesRead != bytesWanted)
        {
            rx_ready = false;  // didn't get all we need
        }
        else if (MSG_HEADER_SIZE == output_index)
        {
            // We have now read the message size header
            // TBD - support other message header formats?
            // (for now, assume 2-byte message length header)
            uint16_t msgSize ;
            memcpy(&msgSize, output_buffer, MSG_HEADER_SIZE);
            output_msg_length = ntohs(msgSize);
        }
        else if (output_index == output_msg_length)
        {
            // Received full message
            rx_needed = false;
            output_index = 0;  // reset for writing to output
            if (output_ready) WriteOutput();
        }
    }
}  // end NormStreamer::RecvData()

void NormStreamer::WriteOutput()
{
    while (output_ready && !rx_needed)
    {
        assert(output_index < output_msg_length);
        ssize_t result = write(output_fd, output_buffer + output_index, output_msg_length - output_index);
        if (result >= 0)
        {
            output_index += result;
            if (output_index == output_msg_length)
            {
                // Complete message written
                rx_needed = true;
                output_index = output_msg_length = 0;
            }
            else
            {
                output_ready = false;
            }
        }
        else
        {
            switch (errno)
            {
                case EINTR:
                    perror("normStreamer output EINTR");
                    continue;  // interupted, try again
                case EAGAIN:
                    // output blocked, wait for next notification
                    //perror("normStreamer output blocked");
                    output_ready = false;
                    break;
                default:
                    perror("normStreamer error writing output");
                    break;
            }
            break;
        }
        /*size_t result = fwrite(output_buffer + output_index, 1, output_msg_length - output_index, output_file);
        if (result > 0)
        {
            output_index += result;
            if (output_index == output_msg_length)
            {
                // Complete message written
                fflush(output_file);
                rx_needed = true;
                output_index = output_msg_length = 0;
            }
            else
            {
                output_ready = false;
            }
        }
        else
        {
            if (feof(output_file))
            {
                // end-of-file reached, TBD - stop acting as receiver, signal sender we're done?
                fprintf(stderr, "normStreamer: output end-of-file detected ...\n");
                output_ready = false;
            }
            else if (ferror(output_file))
            {
                fprintf(stderr, "normStreamer: output error detected ...\n");
                switch (errno)
                {
                    case EINTR:
                        perror("normStreamer output EINTR");
                        continue;  // interupted, try again
                    case EAGAIN:
                        // output blocked, wait for next notification
                        perror("normStreamer output blocked");
                        output_ready = false;
                        break;
                    default:
                        perror("normStreamer error writing output");
                        break;
                }
            }
            return;
        }
        */
    }
}  // end NormStreamer::WriteOutput()

void NormStreamer::HandleNormEvent(const NormEvent& event)
{
    bool logAllocs = false;
    switch (event.type)
    {
        case NORM_TX_QUEUE_EMPTY:
        case NORM_TX_QUEUE_VACANCY:
            tx_ready = true;
            break;
            
        case NORM_GRTT_UPDATED:
            //fprintf(stderr, "new GRTT = %lf\n", NormGetGrttEstimate(norm_session));
            break;
            
        case NORM_TX_WATERMARK_COMPLETED:
            if (NORM_ACK_SUCCESS == NormGetAckingStatus(norm_session))
            {
                //fprintf(stderr, "WATERMARK COMPLETED\n");
                if (tx_watermark_pending)
                {
                    // Flow control ack request was pending.
                    tx_watermark_pending = false;
                    tx_stream_buffer_count -= (tx_stream_buffer_max >> 1);
                }
            }
            else
            {
                // TBD - we could see who didn't ACK and possibly remove them
                //       from our acking list.  For now, we are infinitely
                //       persistent by resetting watermark ack request
                NormResetWatermark(norm_session);
            }
            break; 
            
        case NORM_TX_OBJECT_PURGED:
            // tx_stream graceful close completed
            NormStopSender(norm_session);
            tx_stream = NORM_OBJECT_INVALID;
            if (NORM_OBJECT_INVALID == rx_stream) Stop();
            break;
        
        case NORM_REMOTE_SENDER_INACTIVE:
            //fprintf(stderr, "REMOTE SENDER INACTIVE node: %u\n", NormNodeGetId(event.sender));
            //NormNodeDelete(event.sender);
            //logAllocs = true;
            break;
            
        case NORM_RX_OBJECT_NEW:
            if ((NORM_OBJECT_INVALID == rx_stream) &&
                (NORM_OBJECT_STREAM == NormObjectGetType(event.object)))
            {
                rx_stream = event.object;
                rx_ready = true;
                msg_sync = false;
                rx_needed = true;
                output_index = output_msg_length = 0;
            }
            else
            {
                fprintf(stderr, "normStreamer warning: NORM_RX_OBJECT_NEW while already receiving?!\n");
            }
            
        case NORM_RX_OBJECT_UPDATED:
            rx_ready = true;
            break;
        
        case NORM_RX_OBJECT_ABORTED:
            //fprintf(stderr, "NORM_RX_OBJECT_ABORTED\n");// %hu\n", NormObjectGetTransportId(event.object));
            //logAllocs = true;
            break;
            
        case NORM_RX_OBJECT_COMPLETED:
            // Rx stream has closed 
            // TBD - set state variables so any pending output is
            //       written out and things shutdown if not sender, too
            rx_ready = false;
            break;
            
        default:
            break;     
    }
    //NormReleasePreviousEvent(NormGetInstance(norm_session));

    if (logAllocs) 
    {
#ifdef USE_PROTO_CHECK
        ProtoCheckLogAllocations(stderr);
#endif // USE_PROTO_CHECK
    }
            
}  // end NormStreamer::HandleNormEvent()


void Usage()
{
    fprintf(stderr, "Usage: normStreamer id <nodeId> {send | recv} [addr <addr>[/<port>]][ack <node1>[,<node2>,...]\n"
                    "                    [cc|cce|ccl|rate <bitsPerSecond>][interface <name>][debug <level>][trace]\n"
                    "                    [listen [<mcastAddr>/]<port>][relay <dstAddr>/<port>]\n"
                    "                    [omit][silent][txloss <lossFraction>]\n");
}
int main(int argc, char* argv[])
{
    // REQUIRED parameters initiailization
    NormNodeId nodeId = NORM_NODE_NONE;
    bool send = false;
    bool recv = false;
    
    char sessionAddr[64];
    strcpy(sessionAddr, "224.1.2.3");
    unsigned int sessionPort = 6003;
    
    char listenAddr[64];  // optional mcast addr to listen for input
    listenAddr[0] = '\0';
    unsigned int listenPort = 0;  // optional UDP port to listen for input
    
    char relayAddr[64]; // optional UDP destination address/port to relay output
    relayAddr[0] = '\0';
    unsigned int relayPort = 0;
    
    NormNodeId ackingNodeList[256]; 
    unsigned int ackingNodeCount = 0;
    
    double txRate = 0.0; // used for non-default NORM_FIXED ccMode
    NormStreamer::CCMode ccMode = NormStreamer::NORM_CC;
    const char* mcastIface = NULL;
    
    int debugLevel = 0;
    bool trace = false;
    bool omitHeaderOnOutput = false;
    bool silentReceiver = false;
    double txloss = 0.0;
    
    // Parse command-line
    int i = 1;
    while (i < argc)
    {
        const char* cmd = argv[i++];
        size_t len = strlen(cmd);
        if (0 == strncmp(cmd, "send", len))
        {
            send = true;
        }
        else if (0 == strncmp(cmd, "recv", len))
        {
            recv = true;
        }
        else if (0 == strncmp(cmd, "addr", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "nodeMsgr error: missing 'addr[/port]' value!\n");
                Usage();
                return -1;
            }
            const char* addrPtr = argv[i++];
            const char* portPtr = strchr(addrPtr, '/');
            if (NULL == portPtr)
            {
                strncpy(sessionAddr, addrPtr, 63);
                sessionAddr[63] = '\0';
            }
            else
            {
                size_t addrLen = portPtr - addrPtr;
                if (addrLen > 63) addrLen = 63;  // should issue error message
                strncpy(sessionAddr, addrPtr, addrLen);
                sessionAddr[addrLen] = '\0';
                portPtr++;
                sessionPort = atoi(portPtr);
            }
        }
        else if (0 == strncmp(cmd, "listen", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "nodeMsgr error: missing '[mcastAddr/]port]' value!\n");
                Usage();
                return -1;
            }
            const char* addrPtr = argv[i++];
            const char* portPtr = strchr(addrPtr, '/');
            if (NULL != portPtr)
            {
                size_t addrLen = portPtr - addrPtr;
                if (addrLen > 63) addrLen = 63;  // should issue error message
                strncpy(listenAddr, addrPtr, addrLen);
                listenAddr[addrLen] = '\0';
                portPtr++;
                listenPort = atoi(portPtr);
            }
            else
            {
                // no address, just port
                listenPort = atoi(addrPtr);
            }
        }
        else if (0 == strncmp(cmd, "relay", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "nodeMsgr error: missing relay 'dstAddr/port]' value!\n");
                Usage();
                return -1;
            }
            const char* addrPtr = argv[i++];
            const char* portPtr = strchr(addrPtr, '/');
            if (NULL == portPtr)
            {
                fprintf(stderr, "nodeMsgr error: missing relay 'port' value!\n");
                Usage();
                return -1;
            }
            if (NULL != portPtr)
            {
                size_t addrLen = portPtr - addrPtr;
                if (addrLen > 63) addrLen = 63;  // should issue error message
                strncpy(relayAddr, addrPtr, addrLen);
                relayAddr[addrLen] = '\0';
                portPtr++;
                relayPort = atoi(portPtr);
            }
        }
        else if (0 == strncmp(cmd, "id", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "nodeMsgr error: missing 'id' value!\n");
                Usage();
                return -1;
            }
            nodeId = atoi(argv[i++]);
        }
        else if (0 == strncmp(cmd, "ack", len))
        {
            // comma-delimited acking node id list
            if (i >= argc)
            {
                fprintf(stderr, "nodeMsgr error: missing 'id' <nodeId> value!\n");
                Usage();
                return -1;
            }
            const char* alist = argv[i++];
            while ((NULL != alist) && (*alist != '\0'))
            {
                // TBD - Do we need to skip leading white space?
                if (1 != sscanf(alist, "%d", ackingNodeList + ackingNodeCount))
                {
                    fprintf(stderr, "nodeMsgr error: invalid acking node list!\n");
                    Usage();
                    return -1;
                }
                ackingNodeCount++;
                alist = strchr(alist, ',');
                if (NULL != alist) alist++;  // point past comma
            }
        }
        else if (0 == strncmp(cmd, "rate", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "nodeMsgr error: missing 'rate' <bitsPerSecond> value!\n");
                Usage();
                return -1;
            }
            if (1 != sscanf(argv[i++], "%lf", &txRate))
            {
                fprintf(stderr, "nodeMsgr error: invalid transmit rate!\n");
                Usage();
                return -1;
            }       
            // set fixed-rate operation
            ccMode = NormStreamer::NORM_FIXED;     
        }
        else if (0 == strcmp(cmd, "cc"))
        {
            ccMode = NormStreamer::NORM_CC;
        }
        else if (0 == strcmp(cmd, "cce"))
        {
            ccMode = NormStreamer::NORM_CCE;
        }
        else if (0 == strcmp(cmd, "ccl"))
        {
            ccMode = NormStreamer::NORM_CCL;
        }
        else if (0 == strncmp(cmd, "interface", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "nodeMsgr error: missing 'interface' <name>!\n");
                Usage();
                return -1;
            }
            mcastIface = argv[i++];
        }
        else if (0 == strncmp(cmd, "omit", len))
        {
            omitHeaderOnOutput = true;
        }
        else if (0 == strncmp(cmd, "silent", len))
        {
            silentReceiver = true;
        }
        else if (0 == strncmp(cmd, "txloss", len))
        {
            if (1 != sscanf(argv[i++], "%lf", &txloss))
            {
                fprintf(stderr, "nodeMsgr error: invalid 'txloss' value!\n");
                Usage();
                return -1;
            }
        }
        else if (0 == strncmp(cmd, "debug", len))
        {
            if (i >= argc)
            {
                fprintf(stderr, "nodeMsgr error: missing 'interface' <name>!\n");
                Usage();
                return -1;
            }
            debugLevel = atoi(argv[i++]);
        }
        else if (0 == strncmp(cmd, "trace", len))
        {
            trace = true;
        }
        else if (0 == strncmp(cmd, "help", len))
        {
            Usage();
            return 0;
        }
        else
        {
            fprintf(stderr, "nodeMsgr error: invalid command \"%s\"!\n", cmd);
            Usage();
            return -1;
        }
    }
    
    if (!send && !recv)
    {
        fprintf(stderr, "normStreamer error: not configured to send or recv!\n");
        Usage();
        return -1;
    }
    if (NORM_NODE_NONE == nodeId)
    {
        fprintf(stderr, "normStreamer error: no local 'id' provided!\n");
        Usage();
        return -1;
    }
    
    // TBD - should provide more error checking of calls
    NormInstanceHandle normInstance = NormCreateInstance();
    NormSetDebugLevel(debugLevel);
    
    NormStreamer normStreamer;
    
    if (omitHeaderOnOutput) normStreamer.OmitHeader(true);
    
    if (!normStreamer.OpenNormSession(normInstance, sessionAddr, sessionPort, (NormNodeId)nodeId))
    {
        fprintf(stderr, "normStreamer error: unable to open NORM session\n");
        NormDestroyInstance(normInstance);
        return false;
    }
    
    if (silentReceiver) normStreamer.SetSilentReceiver(true);
    if (txloss > 0.0) normStreamer.SetTxLoss(txloss);
    
    for (unsigned int i = 0; i < ackingNodeCount; i++)
        normStreamer.AddAckingNode(ackingNodeList[i]);
    
    normStreamer.SetNormCongestionControl(ccMode);
    if (NormStreamer::NORM_FIXED == ccMode)
        normStreamer.SetNormTxRate(txRate);
    if (NULL != mcastIface)
        normStreamer.SetNormMulticastInterface(mcastIface);
    
    if (trace) normStreamer.SetNormMessageTrace(true);
    
    // TBD - set NORM session parameters
    normStreamer.Start(send, recv); 
    
    int normfd = NormGetDescriptor(normInstance);
    // Get input/output descriptors and set to non-blocking i/o
    int inputfd = normStreamer.GetInputFile();
    if (-1 == fcntl(inputfd, F_SETFL, fcntl(inputfd, F_GETFL, 0) | O_NONBLOCK))
        perror("normStreamer: fcntl(inputfd, O_NONBLOCK) error");
    int outputfd = normStreamer.GetOutputFile();
    if (-1 == fcntl(outputfd, F_SETFL, fcntl(outputfd, F_GETFL, 0) | O_NONBLOCK))
        perror("normStreamer: fcntl(outputfd, O_NONBLOCK) error");
    fd_set fdsetInput, fdsetOutput;
    FD_ZERO(&fdsetInput);
    FD_ZERO(&fdsetOutput);
    while (normStreamer.IsRunning())
    {
        int maxfd = -1;
        if ((normStreamer.TxPending() && !normStreamer.TxReady()) || 
            (normStreamer.RxNeeded() && !normStreamer.RxReady()))
            
        {
            // we need to wait until NORM is tx_ready or rx_ready
            FD_SET(normfd, &fdsetInput);
            maxfd = normfd;
        }
        if (normStreamer.InputNeeded())
        {   
            FD_SET(inputfd, &fdsetInput);
            if (inputfd > maxfd) maxfd = inputfd;
        }   
        else
        {
            FD_CLR(inputfd, &fdsetInput);
        }
        int result;
        if (normStreamer.OutputPending() && !normStreamer.OutputReady())
        {
            FD_SET(outputfd, &fdsetOutput);
            if (outputfd > maxfd) maxfd = outputfd;
            result = select(maxfd+1, &fdsetInput, &fdsetOutput, NULL, NULL);
        }
        else
        {   
            FD_CLR(outputfd, &fdsetOutput);
            if (maxfd >= 0)
                result = select(maxfd+1, &fdsetInput, NULL, NULL, NULL);
            else
                result = 1;  // pass through (not waiting on any descriptors)
        }
        switch (result)
        {
            case -1:
                switch (errno)
                {
                    case EINTR:
                    case EAGAIN:
                        continue;
                    default:
                        perror("normStreamer select() error");
                        // TBD - stop NormStreamer
                        break;
                }
                break;
            case 0:
                // shouldn't occur for now (no timeout)
                continue;
            default:
                if (FD_ISSET(inputfd, &fdsetInput))
                {
                    normStreamer.SetInputReady();
                }   
                if (FD_ISSET(outputfd, &fdsetOutput))
                {
                    normStreamer.SetOutputReady();
                }
                /*if (FD_ISSET(normfd, &fdsetInput))
                {
                    NormEvent event;
                    while (NormGetNextEvent(normInstance, &event, false))
                        normStreamer.HandleNormEvent(event);
                }*/ 
                break; 
        }
        // We always clear out/handle pending NORM API events
        // (to keep event queue from building up)
        NormEvent event;
        while (NormGetNextEvent(normInstance, &event, false))
            normStreamer.HandleNormEvent(event);
        // As a result of input/output ready or NORM notification events:
        // 1) Recv from rx_stream if needed and ready
        if (normStreamer.RxNeeded() && normStreamer.RxReady())
            normStreamer.RecvData(); 
        // 2) Write any pending data to output if output is ready
        if (normStreamer.OutputPending() && normStreamer.OutputReady())
            normStreamer.WriteOutput();  
        // 3) Read from input if needed and ready 
        if (normStreamer.InputNeeded() && normStreamer.InputReady())
            normStreamer.ReadInput(); 
        // 4) Send any pending tx message
        if (normStreamer.TxPending() && normStreamer.TxReady())
            normStreamer.SendData();
    }  // end while(normStreamer.IsRunning()
    
    fprintf(stderr, "normStreamer exiting ...\n");
    
    fflush(stderr);
    
    NormDestroyInstance(normInstance);
    
}  // end main()

